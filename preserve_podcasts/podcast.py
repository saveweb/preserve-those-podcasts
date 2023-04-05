import dataclasses
import feedparser
import json
import time
from typing import Optional, Union

from preserve_podcasts.utils.type_check import runtimeTypeCheck


@dataclasses.dataclass
class Podcast:
    _Dic = {
        'id': None, # unique id for podcasts archive project
        'enabled': True,

        'title': None,
        'link': None,
        'feed_url': None,
        'summary': None,
        'language': None,
        
        'saveweb': {
            'created_timestamp': 0,
            'last_success_timestamp': 0,
            'last_checked_timestamp': 0,
            'last_checked_status': 'success',
        },
        'ids': {
            'saveweb_box_id': None, # https://box.othing.xyz    int
            'moon.fm': None, # https://moon.fm                  int
            'pod.link': None, # https://pod.link                int
            'google_podcasts': None,
            'apple_podcasts': None,
            'spotify': None,
            'xiaoyuzhoufm': None, # https://xiaoyuzhoufm.com
        }
    }

    def __init__(self):
        self._Dic = Podcast._Dic

    def __getitem__(self, key):
        return self._Dic[key]
    
    def __setitem__(self, key, value):
        if key in self._Dic:
            self._Dic[key] = value
        else:
            raise KeyError(f'Key {key} not found in podcast class')

    def __delitem__(self, key):
        del self._Dic[key]

    def __str__(self) -> str:
        return self.to_json()

    def get(self, key, default=None):
        return self._Dic.get(key, default)


    @runtimeTypeCheck()
    def create(self, init_id: int, init_feed_url: str, init_dic: Optional[dict] = None):
        if init_dic is not None:
            self.load(init_dic)
        self._Dic['saveweb']['created_timestamp'] = int(time.time())
        if self._Dic['id'] is None:
            self._Dic['id'] = init_id
        if self._Dic['feed_url'] is None:
            self._Dic['feed_url'] = init_feed_url


    @runtimeTypeCheck(raise_exception=True)
    def load(self, dic_or_dicFilePath: Union[dict, str]):
        if type(dic_or_dicFilePath) == str:
            dic = json.loads(open(dic_or_dicFilePath, 'r', encoding='utf-8').read())
        else:
            dic = dic_or_dicFilePath

        for key in dic:
            if key in self._Dic:
                self._Dic[key] = dic[key] # type: ignore @runtimeTypeCheck

    def update_failed(self):
        self._Dic['saveweb']['last_checked_timestamp'] = int(time.time())
        self._Dic['saveweb']['last_checked_status'] = 'failed'

    def update_success(self):
        self._Dic['saveweb']['last_checked_timestamp'] = int(time.time())
        self._Dic['saveweb']['last_success_timestamp'] = int(time.time())
        self._Dic['saveweb']['last_checked_status'] = 'success'

    def to_dict(self):
        __Dic = self._Dic
        if 'bozo_exception' in __Dic:
            del(__Dic['bozo_exception'])
        return __Dic
    
    def to_json(self):
        return json.dumps(self._Dic, indent=4, ensure_ascii=False)
    
    def to_json_file(self, file_path: str):
        with open(file_path, 'w') as f:
            f.write(self.to_json())