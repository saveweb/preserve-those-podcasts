import builtins
import fcntl
import sys
import requests
import rich

import resource

from preserve_podcasts.utils.response import get_content_disposition, get_content_length, get_content_type, get_etag, get_last_modified, float_last_modified, get_suggested_filename
from preserve_podcasts.utils.type_check import runtimeTypeCheck
from preserve_podcasts.utils.util import safe_chars

# Limit memory usage
resource.setrlimit(resource.RLIMIT_AS, (1024 * 1024 * 512, 1024 * 1024 * 1024))
# print(resource.getrlimit(resource.RLIMIT_AS))

from typing import List, Optional
import os
import time
import json
import hashlib
import subprocess
from urllib.parse import urlparse

import feedparser
from charset_normalizer import from_bytes

from .podcast import Podcast
from .pod_sessiosn import createSession
from .exception import FeedTooLargeError


DEBUG_MODE = False

# Use rich.print() instead of builtins.print()
USE_RICH_PRINT = True
if USE_RICH_PRINT:
    from rich import print
else:
    rich.print = builtins.print

# Limit Feed size to 20 MiB
FEED_SIZE_LIMIT = 1024 * 1024 * 20 # 20 MiB
MAX_EPISODE_AUDIO_SIZE = 1024 * 1024 * 512 # 512 MiB

# Maximum tolerable file size overestimation rate
MAX_EPISODE_AUDIO_SIZE_TOLERANCE = 3


DATA_DIR = 'data/'
PODCAST_INDEX_DIR = 'podcasts_index/'
PODCAST_AUDIO_DIR = 'podcasts_audio/'
PODCAST_JSON_PREFIX = 'podcast_'
PODCAST_FEED_URL_CACHE = 'feed_url_cache.json'
__DEMO__PODCAST_JSON_FILE = DATA_DIR + PODCAST_INDEX_DIR + PODCAST_JSON_PREFIX + '114514_abcdedfdsf.json'
__DEMO__PODCAST_AUDIO_FILE = DATA_DIR + PODCAST_AUDIO_DIR + '114514/guid_sha1_aabbcc/ep123.mp3'
LOCK_FILE = 'preserve_podcasts.lock'

 # title mark
TITLE_MARK_PREFIX = '=TITLE=='
TITLE_MARK_SUFFIX = '.mark'

EPISODE_DOWNLOAD_CHUNK_SIZE = 1024 * 337 # bytes

def checkFeedSize(data: bytes):
    if data is None:
        return
    if len(data) > FEED_SIZE_LIMIT:
        raise FeedTooLargeError('Feed too large')


def sha1(data: bytes):
    sha1 = hashlib.sha1()
    sha1.update(data)
    return sha1.hexdigest()


def sha1file(file_path: str):
    with open(file_path, 'rb') as f:
        sha1 = hashlib.sha1()
        while True:
            data = f.read(1024)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()

def md5file(file_path: str):
    with open(file_path, 'rb') as f:
        md5 = hashlib.md5()
        while True:
            data = f.read(1024)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()

def is_playable(file_path: str):
    if audio_duration(file_path) > 0:
        return True
    
    return False
    
    
def audio_duration(file_path: str):
    ''' Return audio duration in seconds, -1 if failed'''
    if not os.path.exists(file_path):
        raise FileNotFoundError(f'File not found: {file_path}')

    try:
        t = subprocess.check_output(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path], stderr=subprocess.STDOUT)
        duration = int(t.decode('utf-8').strip("\n").split('.')[0])
        return duration
    except:
        return -1


def checkEpisodeAudioSize(data, possible_sizes: List[int]=[-1]):
    ''' :data: bytes or int'''
    if type(data) == int:
        data_size = data
    elif type(data) == bytes:
        data_size = len(data)
    else:
        raise TypeError('data must be bytes or int')
    possible_size = max(possible_sizes) if max(possible_sizes) > 0 else MAX_EPISODE_AUDIO_SIZE
    if possible_size > MAX_EPISODE_AUDIO_SIZE:
        raise FeedTooLargeError('Episode audio too large')
    if data_size > MAX_EPISODE_AUDIO_SIZE:
        raise FeedTooLargeError('Episode audio too large')
    if possible_size > 0 and data_size > possible_size * MAX_EPISODE_AUDIO_SIZE_TOLERANCE:
        raise FeedTooLargeError('Episode audio too large')
    
    # show progress bar
    print(f'{data_size}/{possible_size} \t {data_size/1024/1024:.2f} MiB {data_size/possible_size*100:.2f}%',
        end='               \r')



def get_feed(session: requests.Session, url: str) -> Optional[bytes]:
    session.stream = True
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        data_raw: Optional[bytes] = None
        encoding = r.encoding
        for chunk in r.iter_content(chunk_size=1024):
            checkFeedSize(chunk)
            if data_raw is None:
                data_raw = chunk
            else:
                data_raw += chunk
        if type(data_raw) != bytes:
            return None
        apparent_encoding = from_bytes(data_raw).best()
        if apparent_encoding is not None:
            apparent_encoding = apparent_encoding.encoding
    print(encoding, apparent_encoding)

    return data_raw




@runtimeTypeCheck()
def download_episode(session: requests.Session, url: str, guid: str, episode_dir: str, filename: str,
                    possible_size: int=-1, title: str= '',
                    force_redownload: bool = False):
    to_download = True
    possible_sizes = [possible_size]

    ep_file_path = os.path.join(episode_dir, filename)
    if os.path.exists(ep_file_path+'.metadata.json'):
        with open(ep_file_path+'.metadata.json', 'r') as f:
            metadata = json.load(f)
            possible_sizes.append(metadata['http-content-length']) if 'http-content-length' in metadata else None

    if os.path.exists(ep_file_path) and os.path.getsize(ep_file_path) in possible_sizes:
        print('File already exists')
        to_download = False
        return

    checkEpisodeAudioSize(0, possible_sizes) # show progress bar and check size
    print('')

    session.stream = True
    with session.get(url, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        # show redirect history
        print('redirect history:')
        for redirect in r.history:
            print(redirect.status_code, '==>', redirect.url)
        print(r.status_code, '==>', r.url)
        content_length = get_content_length(r)
        etag = get_etag(r)
        last_modified = get_last_modified(r)

        content_disposition = get_content_disposition(r)
        suggested_filename = get_suggested_filename(r)

        print(f'content-length: {content_length}, etag: [green]{etag}[/green], last-modified: [yellow]{last_modified}[/yellow], suggested_filename: [blue]{suggested_filename}[/blue]')
        if content_length > 0 and content_length != possible_size:
            if os.path.exists(ep_file_path) and os.path.getsize(ep_file_path) == content_length:
                print('File already exists')
                possible_sizes.append(content_length) if content_length > 0 and content_length not in possible_sizes else None
                to_download = False

        real_size = 0
        if to_download or force_redownload:
            os.makedirs(os.path.dirname(ep_file_path), exist_ok=True)
            with open(ep_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=EPISODE_DOWNLOAD_CHUNK_SIZE):
                    real_size += len(chunk)
                    checkEpisodeAudioSize(real_size, [possible_size, content_length])
                    f.write(chunk)
            print('') # new line

            # create title mark file
            safe_title = safe_chars(title)
            if safe_title and not os.path.exists(os.path.join(episode_dir, TITLE_MARK_PREFIX+safe_title+TITLE_MARK_SUFFIX)):
                with open(os.path.join(episode_dir, TITLE_MARK_PREFIX+safe_title+TITLE_MARK_SUFFIX),
                          'w', encoding='utf-8') as f:
                    if type(title) == str and title:
                        f.write(title)
                    else:
                        f.write('')

            print('\nAudio duration:', audio_duration(ep_file_path))

            save_audio_file_metadata(
                audio_path=ep_file_path, metadata_path=ep_file_path + '.metadata.json', r=r,
                renew=True)

        # modify file modification time
        if last_modified:
            atime = os.path.getatime(ep_file_path) # keep access time
            mtime = float_last_modified(last_modified)
            if mtime:
                os.utime(ep_file_path, (atime, mtime)) # modify file update time
            else:
                print('mtime error:', mtime)


def save_entry(entry:dict, file_path:str):
    with open(file_path, 'w') as f:
        f.write(json.dumps(entry, indent=4, ensure_ascii=False))


@runtimeTypeCheck()
def save_podcast_index_json(podcast: Podcast, podcast_json_file_path: Optional[str]=None):
    if podcast.get('id') is None:
        raise ValueError('No id')

    if podcast_json_file_path is None:
        podcast_json_file_path = os.path.join(DATA_DIR, PODCAST_INDEX_DIR, PODCAST_JSON_PREFIX + str(podcast['id']) + '_' + podcast['title'][:30] + '.json')
        os.makedirs(os.path.dirname(podcast_json_file_path), exist_ok=True)

    with open(podcast_json_file_path, 'w') as f:
        f.write(json.dumps(podcast.to_dict(), indent=4, ensure_ascii=False))

def save_audio_file_metadata(
        audio_path: str, metadata_path: str, r: requests.Response,
        renew: bool = False
        ):
    ''' renew: re calculate sha1, md5 '''
    content_length = get_content_length(r)

    if not renew and os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            old_metadata = json.load(f)
    else:
        old_metadata = {}
    
    url_history = {}
    for i, redirect in enumerate(r.history):

        # del 'Set-Cookie'
        _headers = dict(redirect.headers)
        if 'Set-Cookie' in _headers:
            del _headers['Set-Cookie']

        url_history[int(i)] = {
            'status_code': redirect.status_code,
            'url': redirect.url,
            'headers': _headers,
        }


    url_history[len(r.history)] = {
        'status_code': r.status_code,
        'url': r.url,
        'headers': dict(r.headers),
    }

    metadata = {
        'http-content-length': content_length if content_length > 0 else None,
        'http-etag': get_etag(r),
        'http-last-modified': get_last_modified(r),
        'http-last-modified-float': float_last_modified(get_last_modified(r)),
        'http-content-type': get_content_type(r),
        'http-content-disposition-raw': get_content_disposition(r), # http header 'content-disposition
        'http-content-disposition-filename': get_suggested_filename(r) , # http header 'content-disposition'
        'actual-size': os.path.getsize(audio_path) if os.path.exists(audio_path) else None,
        'actual-duration': audio_duration(audio_path) if audio_duration(audio_path) > 0 else None,
        'sha1': sha1file(audio_path) if old_metadata.get('sha1') is None else old_metadata.get('sha1'),
        'md5': md5file(audio_path) if old_metadata.get('md5') is None else old_metadata.get('md5'),
        'url-history': url_history,
    }
    with open(metadata_path, 'w') as f:
        f.write(json.dumps(metadata, indent=4, ensure_ascii=False))
    


def do_archive(podcast: Podcast, session: requests.Session):
    try:
        d: feedparser.FeedParserDict = feedparser.parse(podcast['feed_url'])
        if d.get('bozo_exception', None) is not None:
            print('bozo_exception:', d.bozo_exception)
            if len(d.feed) == 0:
                raise d.bozo_exception # type: ignore
        podcast.load(d.feed) # type: ignore @runtimeTypeCheck
    except Exception as e:
        podcast.update_failed()
        raise e

    if DEBUG_MODE:
        os.makedirs('debug', exist_ok=True)
        with open(f'debug/{podcast["id"]}_{int(time.time())}.debug.json', 'w') as f:
            f.write(json.dumps(d, indent=4, ensure_ascii=False))

    podcast_audio_dir = os.path.join(DATA_DIR, PODCAST_AUDIO_DIR, str(podcast['id']))

    archive_entries(d=d, session=session, podcast_audio_dir=podcast_audio_dir)
    podcast.update_success()


@runtimeTypeCheck()
def url2audio_filename(url: str) -> str:
    parsed_url = urlparse(url)
    audio_filename = parsed_url.path.split('/')[-1]
    if '.' not in audio_filename:
        audio_filename += '.mp3'
    if audio_filename == '.mp3':
        audio_filename = 'episode.mp3'

    return audio_filename


def archive_entries(d: feedparser.FeedParserDict, session: requests.Session, podcast_audio_dir: str):
    for entry in d.entries:
        is_episode = False
        for link in entry.get('links', []):
            if link.has_key('type') and 'audio' in link['type']:
                is_episode = True
                break

        if not is_episode:
            continue

        print("\n=====================================")
        print(f'Title: "{entry.get("title")}"')

        title: str = entry.get('title', '') # type: ignore
        safe_title = safe_chars(title) 

        print(f'Safe Title: "{safe_title}"')
        print(f'Link: {entry.get("link")}')
        guid = entry.get('id')
        print(f'GUID: {guid}')
        if guid is None:
            raise ValueError('GUID must exist')
        if type(guid) is not str or guid == '':
            raise ValueError('GUID must be str and not empty')
        print(f'Published: {entry.get("published")}', '\t' ,f'Updated: {entry.get("updated")}')

        itunes_title = entry.get('itunes_title')
        itunes_duration = entry.get('itunes_duration')
        itunes_season = entry.get('itunes_season')
        itunes_episode = entry.get('itunes_episode')
        print(f'itunes_title: [yellow]{itunes_title}[/yellow]')
        print(f'itunes_duration: {itunes_duration}', '\t' ,
              f'itunes_season: {itunes_season}', '\t' ,
              f'itunes_episode: {itunes_episode}')
        itunes_explicit = entry.get('itunes_explicit') # NSFW
        
        for link in entry.links:
            if link.has_key('type') and 'audio' in link.type:
                print(link.href)
                print(link.type)
                print(link.get('length', -1), "(",
                      int(int(link.get('length', -1))/1024/1024), "MiB )") # type: ignore

                sha1ed_guid = sha1(guid.encode('utf-8'))
                episode_dir = os.path.join(podcast_audio_dir, sha1ed_guid)

                download_episode(session, link.href, possible_size=int(link.get('length', -1)), guid=guid, # type: ignore
                                 episode_dir=episode_dir,
                                 filename=url2audio_filename(link.href), # type: ignore @runtimeTypeCheck
                                 title=title,
                )
                save_entry(entry, file_path=os.path.join(episode_dir, f'entry_guid_sha1_{sha1ed_guid}.json'))
                break # avoid downloading multiple audio files


def feed_url_sha1(feed_url: str)-> str:
    ''' return the sha1 of the feed url

    Note: URL ends with `/` will be `rstrip('/')` before hashing
    Note: `http://` URL will be treated as the same `https://` URL before hashing
    '''
    parsed_url = urlparse(feed_url)
    if parsed_url.scheme == 'http':
        parsed_url = parsed_url._replace(scheme='https')
    # print(f'feed_url_sha1: {feed_url} -> {parsed_url.geturl().rstrip("/")}')
    return sha1(parsed_url.geturl().rstrip('/').encode('utf-8'))


def all_feed_url_sha1(use_cache: bool=False)-> dict:
    ''' return a set of all feed url sha1.  {feed_url_sha1<str>: podcast_id<int>}

    Note: URL ends with `/` will be `rstrip('/')` before hashing
    Note: `http://` URL will be treated as the same `https://` URL before hashing
    '''
    podcast_feed_url_cache = {}
    if use_cache:
        with open(os.path.join(DATA_DIR, PODCAST_INDEX_DIR, PODCAST_FEED_URL_CACHE), 'r') as f:
            podcast_feed_url_cache = json.load(f)
            if type(podcast_feed_url_cache) != dict:
                podcast_feed_url_cache = {}
            print(f'all_feed_url_sha1 cache loaded: {len(podcast_feed_url_cache)}')
            return podcast_feed_url_cache

    podcast_index_dirs = os.listdir(DATA_DIR + PODCAST_INDEX_DIR) if os.path.exists(DATA_DIR + PODCAST_INDEX_DIR) else []
    for podcast_idnex_dir in podcast_index_dirs:
        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX):
            podcast_id = int(podcast_idnex_dir[len(PODCAST_JSON_PREFIX):].split('_')[0])
            podcast_json_file_path = os.path.join(DATA_DIR, PODCAST_INDEX_DIR, podcast_idnex_dir)
            with open(podcast_json_file_path, 'r') as f:
                podcast_json = json.load(f)
            feed_url: str = podcast_json['feed_url']
            podcast_feed_url_cache.update({feed_url_sha1(feed_url): podcast_id})
    with open(os.path.join(DATA_DIR, PODCAST_INDEX_DIR, PODCAST_FEED_URL_CACHE), 'w') as f:
        # separators=(',\n', ': '))
        json.dump(podcast_feed_url_cache, f, indent=4, sort_keys=True)
        
        print(f'all_feed_url_sha1 cache refreshed/loaded: {len(podcast_feed_url_cache)}')

    return podcast_feed_url_cache



def add_podcast(session: requests.Session, feed_url: str):
    if feed_url_sha1(feed_url) in all_feed_url_sha1():
        raise ValueError(f'Podcast already exists (sha1: "{feed_url_sha1(feed_url)}")\n')
    podcast_index_dirs = os.listdir(DATA_DIR + PODCAST_INDEX_DIR) if os.path.exists(DATA_DIR + PODCAST_INDEX_DIR) else []
    unavailable_podcast_ids = set()
    for podcast_idnex_dir in podcast_index_dirs:
        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX):
            podcast_id = int(podcast_idnex_dir[len(PODCAST_JSON_PREFIX):].split('_')[0])
            unavailable_podcast_ids.add(podcast_id)
    available_podcast_id = 1
    while available_podcast_id in unavailable_podcast_ids:
        available_podcast_id += 1
    
    this_podcast = Podcast()
    this_podcast.create(init_id=available_podcast_id, init_feed_url=feed_url)
    print(f'Podcast id: {this_podcast.get("id")}')
    do_archive(this_podcast, session=session)
    save_podcast_index_json(this_podcast)
    all_feed_url_sha1()


class ProgramLock:
    def __init__(self, lock_file):
        self.lock_file = lock_file
        self.lock_file_fd = None

    def __enter__(self):
        self.lock_file_fd = open(self.lock_file, 'w')
        try:
            fcntl.lockf(self.lock_file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print("Acquired lock, continuing.")
        except IOError:
            print("Another instance is already running, quitting.")
            sys.exit(-1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file_fd is None:
            raise IOError("Lock file not opened.")
        fcntl.lockf(self.lock_file_fd, fcntl.LOCK_UN)
        self.lock_file_fd.close()
        print("Released lock.")

    # decorator
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper


def get_args():
    import argparse
    parser = argparse.ArgumentParser()
    # parser.add_argument('--debug', action='store_true')
    parser.add_argument('-a','--add', nargs='+', help='RSS feed URL(s)', default=[])
    parser.add_argument('-u','--update', action='store_true', help='Update podcast')

    args = parser.parse_args()
    if args.update and args.add:
        parser.error('--update can not be used with RSS feed URL(s)')
    
    return args

@ProgramLock(LOCK_FILE)
def main():
    session = createSession()
    args = get_args()
    for feed_url in args.add:
        try:
            add_podcast(session, feed_url)
        except ValueError as e:
            if str(e).startswith('Podcast already exists'):
                print(str(e))
            else:
                raise e

    if not args.update:
        return

    podcast_index_dirs = os.listdir(DATA_DIR + PODCAST_INDEX_DIR) if os.path.exists(DATA_DIR + PODCAST_INDEX_DIR) else []
    podcast_ids = {}
    all_feed_url_sha1()
    for podcast_idnex_dir in podcast_index_dirs:
        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX):
            # try:
            podcast_id = int(podcast_idnex_dir[len(PODCAST_JSON_PREFIX):].split('_')[0])
            # except ValueError:
            #     raise ValueError(f'Cant parse podcast id from {podcast_idnex_dir}')

            podcast_ids[podcast_id] = podcast_idnex_dir

    for podcast_id in podcast_ids:
        podcast_idnex_dir = podcast_ids[podcast_id]

        podcast_json_file_path = os.path.join(DATA_DIR, PODCAST_INDEX_DIR, podcast_idnex_dir)
        this_podcast = Podcast()
        this_podcast.load(podcast_json_file_path)
        if podcast_id != this_podcast['id']:
            raise ValueError('Podcast id not match')
        if this_podcast['id'] is None:
            raise ValueError('Podcast id not set')

        do_archive(this_podcast, session=session)
        save_podcast_index_json(this_podcast, podcast_json_file_path=podcast_json_file_path)
    
    all_feed_url_sha1()

if __name__ == '__main__':
    main()