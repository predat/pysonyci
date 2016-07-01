# coding: utf8

import os
import sys
import logging
import requests
import json
from requests.auth import HTTPBasicAuth

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0


SONYCI_URI = "https://api.cimediacloud.com"
SINGLEPART_URI = 'https://io.cimediacloud.com/upload'
MULTIPART_URI = 'https://io.cimediacloud.com/upload/multipart'
CHUNK_SIZE = 10 * 1024 * 1024
USE_THREADS = False


log = logging.getLogger(__name__)


class SonyCiException(Exception):
    def __init__(self, error_code, error_msg):
        self.error_code = error_code
        self.error_msg = error_msg
        Exception.__init__(self, error_code, error_msg)

    def __str__(self):
        return '%s -> %s' % (self.error_code, self.error_msg)


class SonyCi(object):

    def __init__(self, config_path=None):
        if os.path.exists(config_path):
            cfg = ConfigParser()
            cfg.read(config_path)
        else:
            print("Config file not found.")
            sys.exit(1)
        self._authenticate(cfg)

    def _authenticate(self, cfg):
        url = SONYCI_URI + '/oauth2/token'
        data = {'grant_type': 'password',
                'client_id': cfg.get('general', 'client_id'),
                'client_secret': cfg.get('general', 'client_secret')}
        auth = HTTPBasicAuth(cfg.get('general', 'username'),
                             cfg.get('general', 'password'))
        req = requests.post(url, data=data, auth=auth)

        json_resp = req.json()
        log.debug("auth: \n%s" % json.dumps(json_resp, indent=4))

        if req.status_code != requests.codes.ok:
            raise SonyCiException(json_resp['error'],
                                  json_resp['error_description'])

        self.access_token = json_resp['access_token']
        self.header_auth = {'Authorization': 'Bearer %s' % self.access_token}

        if cfg.get('general', 'workspace_id'):
            self.workspace_id = cfg.get('general', 'workspace_id')
        else:
            for w in self.workspaces(fields='name,class'):
                if 'Personal' in w['class']:
                    self.workspace_id = w['id']

    def workspaces(self, limit=50, offset=0, fields='class'):
        url = SONYCI_URI + '/workspaces'
        params = {'limit': limit,
                  'offset': offset,
                  'fields': fields}

        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        log.debug("workspaces: \n%s" % json.dumps(json_resp, indent=4))

        if json_resp['count'] >= 1:
            for el in json_resp['items']:
                yield el

    # def list(self, kind='all', limit=50, offset=0,
    #          fields='description, parentId, folder'):
    def list(self, kind='all', limit=50, offset=0, fields='metadata'):
        if self.workspace_id:
            url = SONYCI_URI + '/workspaces/%s/contents' % self.workspace_id
        else:
            url = SONYCI_URI + '/workspaces'
        params = {'limit': limit,
                  'offset': offset,
                  'kind': kind,
                  'fields': fields}
        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        log.debug("list: \n%s" % json.dumps(json_resp, indent=4))

        return json_resp

    def items(self):
        elts = self.list()
        if elts['count'] >= 1:
            for el in elts['items']:
                log.debug("items: \n%s" % json.dumps(el, indent=4))
                yield el

    def assets(self):
        elts = self.list(kind='asset')
        if elts['count'] >= 1:
            for el in elts['items']:
                log.debug("assets: \n%s" % json.dumps(el, indent=4))
                yield el

    def folders(self):
        elts = self.list(kind='folder')
        if elts['count'] >= 1:
            for el in elts['items']:
                log.debug("folders: \n%s" % json.dumps(el, indent=4))
                yield el

    def search(self, name, limit=50, offset=0, kind="all", workspace_id=None):
        if not workspace_id:
            workspace_id = self.workspace_id

        url = SONYCI_URI + '/workspaces/%s/search' % workspace_id
        params = {'kind': kind,
                  'limit': limit,
                  'offset': offset,
                  'query': name}
        req = requests.get(url, params=params, headers=self.header_auth)
        json_resp = req.json()
        log.debug(json_resp)
        return json_resp

    def upload(self, file_path, folder_id=None, workspace_id=None, metadata={}):
        #if os.path.getsize(file_path) >= 5 * 1024 * 1024:
        log.info('Start multipart upload')
        asset_id = self._initiate_multipart_upload(file_path,
                                                   folder_id,
                                                   workspace_id,
                                                   metadata)
        if USE_THREADS:
            self._do_multipart_upload_part_parallel(file_path, asset_id)
        else:
            self._do_multipart_upload_part(file_path, asset_id)
        return self._complete_multipart_upload(asset_id)
        #else:
        #    return self._singlepart_upload(file_path, folder_id, workspace_id)

    def _initiate_multipart_upload(self, file_path, folder_id=None,
                                   workspace_id=None,
                                   metadata={}):
        data = {'name': os.path.basename(file_path),
                'size': os.path.getsize(file_path),
                'metadata': metadata}

        if folder_id:
            data['folderId'] = folder_id

        if workspace_id:
            data['workspaceId'] = workspace_id
        else:
            data['workspaceId'] = self.workspace_id

        url = MULTIPART_URI
        req = requests.post(url, json=data, headers=self.header_auth)
        json_resp = req.json()
        log.debug("upload: init: %s" % json_resp)
        return json_resp['assetId']

    def _do_multipart_upload_part(self, file_path, asset_id):
        headers = {'Authorization': 'Bearer %s' % self.access_token,
                   'Content-Type': 'application/octet-stream'}
        s = requests.Session()
        part = 0
        with open(file_path, 'rb') as fp:
            while True:
                part = part + 1
                url = MULTIPART_URI + '/%s/%s' % (asset_id, part)
                buf = fp.read(CHUNK_SIZE)
                if not buf:
                    break
                # req = requests.put(url, data=buf, headers=headers)
                req = s.put(url, data=buf, headers=headers)
                resp = req.text
                log.info('upload: part: %s' % part)

    def _do_multipart_upload_part_parallel(self, file_path, asset_id):
        from Queue import Queue
        from threading import Thread

        q = Queue()

        def worker():
            while True:
                headers = {'Authorization': 'Bearer %s' % self.access_token,
                           'Content-Type': 'application/octet-stream'}
                data = q.get()
                req = requests.put(data[0], data=data[1], headers=headers)
                resp = req.text
                log.debug('upload: part: %s' % resp)
                q.task_done()

        for i in range(4):
            t = Thread(target=worker)
            t.setDaemon(True)
            t.start()

        part = 0
        with open(file_path, 'rb') as fp:
            while True:
                part = part + 1
                url = MULTIPART_URI + '/%s/%s' % (asset_id, part)
                buf = fp.read(CHUNK_SIZE)
                if not buf:
                    break
                data = [url, buf]
                q.put(data)
        q.join()

    def _complete_multipart_upload(self, asset_id):
        url = MULTIPART_URI + '/%s/complete' % asset_id
        print(url)
        req = requests.post(url, headers=self.header_auth)
        resp = req.text
        log.debug("upload: complete: %s " % resp)

    def _singlepart_upload(self, file_path, folder_id, workspace_id):
        files = {'file': open(file_path, 'r')}
        req = requests.post(SINGLEPART_URI,
                            files=files, headers=self.header_auth)
        json_resp = req.json()
        log.debug('upload: %s' % json_resp)
        return json_resp['assetId']

    def create_mediabox(self, name, asset_ids, type, allow_download=False,
                        recipients=[], message=None, password=None,
                        expiration_days=None, expiration_date=None,
                        send_notifications=False, notify_on_open=False):
        data = {'name': name,
                'assetIds': asset_ids,
                'type': type,
                'recipients': recipients}

        if message:
            data['message'] = message
        if password:
            data['password'] = password
        if expiration_days:
            data['expirationDays'] = expiration_days
        if expiration_date:
            data['expirationDate'] = expiration_date
        if send_notifications:
            data['sendNotifications'] = 'true'
        if notify_on_open:
            data['notifyOnOpen'] = 'true'

        url = SONYCI_URI + '/mediaboxes'
        req = requests.post(url, json=data, headers=self.header_auth)

        json_resp = req.json()
        log.debug('create_mediabox: %s' % json_resp)
        return json_resp['mediaboxId'], json_resp['link']

    def create_folder(self, name, parent_folder_id=None, workspace_id=None):
        url = SONYCI_URI + '/folders'
        data = {'name': name}
        if parent_folder_id:
            data['parentFolderId'] = parent_folder_id

        if workspace_id:
            data['workspaceId'] = workspace_id
        else:
            data['workspaceId'] = self.workspace_id

        print('----------------------\n%s' % data )

        req = requests.post(url, json=data, headers=self.header_auth)
        json_resp = req.json()
        log.debug('create_folder: %s' % json_resp)
        return json_resp['folderId']

    def detail_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s' % folder_id
        req = requests.get(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('detail_folder: %s' % json_resp)
        return json_resp

    def delete_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s' % folder_id
        req = requests.delete(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('delete_folder: %s' % json_resp)

        if json_resp['message'] == 'Folder was deleted.':
            return True
        else:
            return False

    def trash_folder(self, folder_id):
        url = SONYCI_URI + '/folders/%s/trash' % folder_id
        req = requests.post(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('trash_folder: %s' % json_resp)

        if json_resp['message'] == 'Folder was trashed.':
            return True
        else:
            return False

    def archive(self, asset_id):
        url = SONYCI_URI + '/assets/%s/archive' % asset_id
        req = requests.post(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('archive: %s' % json_resp)

        if json_resp['message'] == 'Asset archive has started.':
            return True
        else:
            return False

    def download(self, asset_id):
        for a in self.assets():
            if asset_id in a['id']:
                name = a['name']

        url = SONYCI_URI + '/assets/%s/download' % asset_id

        req = requests.get(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('download: %s' % json_resp)

        if json_resp['location']:
            req = requests.get(url=json_resp['location'], stream=True)
            with open(name, 'wb') as fp:
                for chunk in req.iter_content(chunk_size=8192):
                    if chunk:
                        fp.write(chunk)

    def delete_asset(self, asset_id):
        url = SONYCI_URI + '/assets/%s'
        req = requests.delete(url, headers=self.header_auth)
        json_resp = req.json()
        log.debug('delete_asset: %s' % json_resp)

        if json_resp['message'] == 'Asset was deleted.':
            return True
        else:
            return False


if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(module)s :: %(asctime)s :: %(levelname)s :: %(message)s')

    # logging to file
    #file_handler = RotatingFileHandler('vantage_to_ci.log', 'a', 1000000, 1)
    #file_handler.setLevel(logging.DEBUG)
    #file_handler.setFormatter(formatter)
    #logger.addHandler(file_handler)

    # logging to the console
    steam_handler = logging.StreamHandler()
    steam_handler.setLevel(logging.DEBUG)
    steam_handler.setFormatter(formatter)
    logger.addHandler(steam_handler)

    #cfg_file = "/Users/predat/Documents/dev/sony_ci/python/sonyci/config/ci_cap.cfg"
    cfg_file = '/tmp/ci_hw.cfg'
    ci = SonyCi(cfg_file)
    # print(ci.access_token)

    # get workspaces
    for w in ci.workspaces(fields='name,class'):
         #if 'Personal' in w['class']:
        print w

    # get folders
    for f in ci.folders():
        #if f['name'] == 'Folder':
        print f

    # get assets
    for a in ci.assets():
        print a

    # for e in ci.items():
    #     print('-' * 80)
    #     pprint(e)

    # create Mediabox
    # m = ci.create_mediabox(name='Test_mediabox',
    #                        asset_ids=['a6679a183d2942e4a9822096a4ede2d0',
    #                                   '0b20f616d4d84b148142618bf5376827'],
    #                        type='Public',
    #                        recipients=['sylvain@predat.fr'],
    #                        expiration_days=5)
    # print(m)

    ci.upload('/Users/predat/Downloads/1080p.mp4')

    # json_resp = ci.search('TEST', kind='folder')
    # test_folder_id = json_resp['items'][0]['id']
    # ci.upload('/Users/predat/Downloads/cosmos.mp4', folder_id=test_folder_id)

    # ci.download(asset_id='0b20f616d4d84b148142618bf5376827')

    #folder_id = ci.create_folder(name='Folder')
    #sub_folder_id = ci.create_folder(name='SubFolder', parent_folder_id=folder_id)

    #print ci.detail_folder('7bd1bde8782a4870a0bbc9ec7b8998be')
