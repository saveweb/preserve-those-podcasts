import dataclasses
import json
from pathlib import Path
import time
from typing import Dict, Optional, Union

from preserve_podcasts.utils.util import podcast_guid_uuid5
from preserve_podcasts.utils.type_check import runtimeTypeCheck


@dataclasses.dataclass
class Podcast:
    _Dic = {
        'id': None, # podcast_guid_uuid5 from feed_url
        'enabled': True,

        'title': None,
        'subtitle': None,
        'link': None,
        'feed_url': None,
        'summary': None,
        'language': None,
        'author': None,
        'image': None,

        'podcast_guid': None, # original guid from podcast:guid

        'saveweb': {
            'created_timestamp': 0,
            'last_success_timestamp': 0,
            'last_checked_timestamp': 0,
            'last_checked_status': 'success',
        },

        'tags': [],
        # tags: [{
        #     "term": "Arts",
        #     "scheme": "http://www.itunes.com/",
        #     "label": None
        # },..]

        # 'ids': {
        #     'apple_podcasts': None,
        #     'spotify': None,
        #     # podcastindex: -> id
        #     'moon.fm': None, # https://moon.fm                  int
        #     'pod.link': None, # https://pod.link                int
        #     'google_podcasts': None,
        #     'xiaoyuzhoufm': None, # https://xiaoyuzhoufm.com
        # }
    }
    @property
    def id(self)->str:          return self._Dic['id']
    @property
    def enabled(self)->bool:    return self._Dic['enabled']
    @property
    def title(self):            return self._Dic['title']
    @property
    def subtitle(self):         return self._Dic['subtitle'] if self._Dic['subtitle'] else None
    @property
    def link(self):             return self._Dic['link']
    @property
    def feed_url(self)->str:    return self._Dic['feed_url']
    @property
    def summary(self):          return self._Dic['summary'] if self._Dic['summary'] else None
    @property
    def language(self):         return self._Dic['language'] if self._Dic['language'] else None
    @property
    def author(self):           return self._Dic['author'] if self._Dic['author'] else None
    @property
    def image(self):            return self._Dic['image'] if self._Dic['image'] else None
    @property
    def podcast_guid(self):     return self._Dic['podcast_guid'] if self._Dic['podcast_guid'] else None
    @property
    def tags(self):             return self._Dic['tags'] if self._Dic['tags'] else {}

    # TODO: class Saveweb
    @property
    def saveweb(self)->Dict:    return self._Dic['saveweb']


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
    def create(self, init_feed_url: str, init_dic: Optional[dict] = None):
        if init_dic is not None:
            self.load(init_dic)
        self._Dic['saveweb']['created_timestamp'] = int(time.time())
        if self._Dic['feed_url'] is None:
            self._Dic['feed_url'] = init_feed_url
        if self._Dic['id'] is None:
            self._Dic['id'] = podcast_guid_uuid5(init_feed_url)


    @runtimeTypeCheck(raise_exception=True)
    def load(self, dic_or_dicFilePath: Union[dict, Path, str]):
        if isinstance(dic_or_dicFilePath, (str, Path)):
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
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())
