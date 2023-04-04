import requests


import resource

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
from rich import print

from .podcast import Podcast
from .pod_sessiosn import createSession
from .exception import FeedTooLargeError


DEBUG_MODE = False

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
    for possible_size in possible_sizes:
        if possible_size > MAX_EPISODE_AUDIO_SIZE:
            raise FeedTooLargeError('Episode audio too large')
        if data_size > MAX_EPISODE_AUDIO_SIZE:
            raise FeedTooLargeError('Episode audio too large')
        if possible_size > 0 and data_size > possible_size * MAX_EPISODE_AUDIO_SIZE_TOLERANCE:
            raise FeedTooLargeError('Episode audio too large')
    
        # show progress bar
        print(f'{data_size}/{possible_size}/{MAX_EPISODE_AUDIO_SIZE} \t {data_size/1024/1024:.2f} MiB {data_size/possible_size*100:.2f}%',
            end='               \r')



def get_feed(session, url) -> Optional[bytes]:
    session.stream = True
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        data_raw: bytes = None
        encoding = r.encoding
        for chunk in r.iter_content(chunk_size=1024):
            checkFeedSize(chunk)
            if data_raw is None:
                data_raw = chunk
            else:
                data_raw += chunk
        apparent_encoding = from_bytes(data_raw).best().encoding
    print(encoding, apparent_encoding)

    return data_raw





def download_episode(session: requests.Session, url, possible_size=-1, guid: str = None, episode_dir: str = None,
                     filename: str = None, force_redownload: bool = False):
    to_download = True
    possible_sizes = [possible_size] if possible_size > 0 else []

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

    session.stream = True
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        content_length = int(r.headers.get('content-length', -1))
        etag = r.headers.get('etag', None)
        last_modified = r.headers.get('last-modified', None)

        suggested_filename = r.headers.get('content-disposition', None)
        if suggested_filename is not None:
            suggested_filename = suggested_filename.split('filename=')[-1]
            if suggested_filename.startswith('"') and suggested_filename.endswith('"'):
                suggested_filename = suggested_filename[1:-1]
            if suggested_filename.startswith("'") and suggested_filename.endswith("'"):
                suggested_filename = suggested_filename[1:-1]
            if suggested_filename:
                pass

        print(f'content-length: {content_length}, etag: {etag}, last-modified: {last_modified}, suggested_filename: {suggested_filename}')
        if content_length > 0 and content_length != possible_size:
            if os.path.exists(ep_file_path) and os.path.getsize(ep_file_path) == content_length:
                print('File already exists')
                possible_sizes.append(content_length) if content_length > 0 and content_length not in possible_sizes else None
                to_download = False
           
        real_size = 0
        if to_download or force_redownload:
            os.makedirs(os.path.dirname(ep_file_path), exist_ok=True)
            with open(ep_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 256): # 256 KiB
                    real_size += len(chunk)
                    checkEpisodeAudioSize(real_size, [possible_size, content_length])
                    f.write(chunk)

            print('\nAudio duration:', audio_duration(ep_file_path))

            save_audio_file_metadata(content_length=content_length, etag=etag, last_modified=last_modified,
                                audio_path=ep_file_path, metadata_path=ep_file_path + '.metadata.json',
                                suggested_filename=suggested_filename,
                                renew=True)

        # modify file update time
        if last_modified:
            atime = os.path.getatime(ep_file_path)
            mtime = time.mktime(time.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z'))
            os.utime(ep_file_path, (atime, mtime))

def save_entry(entry:dict, file_path:str):
    with open(file_path, 'w') as f:
        f.write(json.dumps(entry, indent=4, ensure_ascii=False))


def save_podcast_index_json(podcast: Podcast, podcast_json_file_path: str=None):
    if podcast.get('id') is None:
        raise ValueError('No id')

    if podcast_json_file_path is None:
        podcast_json_file_path = os.path.join(DATA_DIR, PODCAST_INDEX_DIR, PODCAST_JSON_PREFIX + str(podcast['id']) + '_' + podcast['title'][:30] + '.json')
        os.makedirs(os.path.dirname(podcast_json_file_path), exist_ok=True)

    with open(podcast_json_file_path, 'w') as f:
        f.write(json.dumps(podcast.to_dict(), indent=4, ensure_ascii=False))

def save_audio_file_metadata(
        content_length: int, etag: str, last_modified: str, audio_path: str, metadata_path: str, suggested_filename: str = None,
        renew: bool = False
        ):

    if not renew and os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            old_metadata = json.load(f)
    else:
        old_metadata = {}

    metadata = {
        'http-content-length': content_length if content_length > 0 else None,
        'http-etag': etag,
        'http-last-modified': last_modified,
        'http-suggested-filename': suggested_filename , # http header 'content-disposition'
        'actual-size': os.path.getsize(audio_path) if os.path.exists(audio_path) else None,
        'actual-duration': audio_duration(audio_path) if audio_duration(audio_path) > 0 else None,
        'sha1': sha1file(audio_path) if old_metadata.get('sha1') is None else old_metadata.get('sha1'),
        'md5': md5file(audio_path) if old_metadata.get('md5') is None else old_metadata.get('md5'),
    }
    with open(metadata_path, 'w') as f:
        f.write(json.dumps(metadata, indent=4, ensure_ascii=False))
    


def do_archive(podcast: Podcast, session: requests.Session):
    try:
        d: feedparser.FeedParserDict = feedparser.parse(podcast['feed_url'])
        if d.get('bozo_exception', None) is not None:
            raise d.bozo_exception 
        podcast.load(d.feed)
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


def archive_entries(d: feedparser.FeedParserDict, session: requests.Session, podcast_audio_dir: str):
    ntfs_chars = r'<>:"/\|?*'
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
        safe_title = entry.get('title', '').translate({ord(c): None for c in ntfs_chars})[:40]
        print(f'Safe Title: "{safe_title}"')
        print(f'Link: {entry.get("link")}')
        print(f'GUID: {entry["id"]}') # guid must exist
        print(f'Published: {entry.get("published")}')
        print(f'Updated: {entry.get("updated")}')
        print(f'itunes_duration: {entry.get("itunes_duration")}')
        # itunes_title = entry.get('itunes_title')
        itunes_season = entry.get('itunes_season')
        itunes_episode = entry.get('itunes_episode')
        print(f'itunes_season: {itunes_season}, itunes_episode: {itunes_episode}')
        itunes_explicit = entry.get('itunes_explicit') # NSFW
        
        for link in entry.links:
            if link.has_key('type') and 'audio' in link.type:
                print(link.href)
                print(link.type)
                print(link.get('length', -1), "(", int(int(link.get('length', -1))/1024/1024), "MiB )")

                parsed_url = urlparse(link.href)
                audio_filename = parsed_url.path.split('/')[-1]
                if '.' not in audio_filename:
                    audio_filename += '.mp3'
                if audio_filename == '.mp3':
                    audio_filename = 'episode.mp3'

                sha1ed_guid = sha1(entry["id"].encode('utf-8'))
                episode_dir = os.path.join(podcast_audio_dir, sha1ed_guid)

                download_episode(session, link.href, possible_size=int(link.get('length', -1)), guid=entry["id"], episode_dir=episode_dir, filename=audio_filename)
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


def all_feed_url_sha1(use_cache: bool=False)-> set:
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
        raise ValueError('Podcast already exists')
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



def get_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--add', type=str, default=None)

    return parser.parse_args()


def main():
    global DEBUG_MODE
    session = createSession()
    args = get_args()
    DEBUG_MODE = args.debug
    if args.add:
        add_podcast(session=session, feed_url=args.add)
        return

    podcast_index_dirs = os.listdir(DATA_DIR + PODCAST_INDEX_DIR) if os.path.exists(DATA_DIR + PODCAST_INDEX_DIR) else []
    podcast_ids = {}
    all_feed_url_sha1()
    for podcast_idnex_dir in podcast_index_dirs:
        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX):
            podcast_id = int(podcast_idnex_dir[len(PODCAST_JSON_PREFIX):].split('_')[0])
            podcast_ids[podcast_id] = podcast_idnex_dir

        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX + str(podcast_id) + '_'):
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