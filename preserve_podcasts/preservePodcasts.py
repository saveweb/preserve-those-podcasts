import builtins
from pathlib import Path
import logging
import shutil

import rich
import requests
from requests.structures import CaseInsensitiveDict

from preserve_podcasts.utils.file import audio_duration, md5file, sha1file
from preserve_podcasts.utils.fileLock import AlreadyRunningError, FileLock
from preserve_podcasts.utils.response import get_content_disposition, get_content_length, get_content_type, get_etag, get_last_modified, float_last_modified, get_suggested_filename
from preserve_podcasts.utils.type_check import runtimeTypeCheck
from preserve_podcasts.utils.util import podcast_guid_uuid5, safe_chars, sha1

logger = logging.getLogger(__name__)

from typing import Dict, List, Optional, Set
import os
import time
import json
from urllib.parse import urlparse

import feedparser
from charset_normalizer import from_bytes

from .podcast import Podcast
from .pod_sessiosn import PRESERVE_THOSE_POD_UA, create_session
from .exception import FeedTooLargeError


DEBUG_MODE = False

# Use rich.print() instead of builtins.print()
USE_RICH_PRINT = True
if USE_RICH_PRINT:
    from rich import print
else:
    rich.print = builtins.print
    print = builtins.print

assert print

# Limit Feed size to 20 MiB
FEED_SIZE_LIMIT = 1024 * 1024 * 40 # 40 MiB
MAX_EPISODE_AUDIO_SIZE = 1024 * 1024 * 778 # 778 MiB

# Maximum tolerable file size overestimation rate
MAX_EPISODE_AUDIO_SIZE_TOLERANCE = 3


DATA_DIR = Path('pod_data/')
PODCAST_INDEX_DIR = 'podcasts_index/'
PODCAST_LOCK_DIR = 'podcasts_lock/'
PODCAST_AUDIO_DIR = 'podcasts_audio/'
PODCAST_JSON_PREFIX = 'podcast_'
PODCAST_ID_CACHE = 'feed_id_cache.txt'
__DEMO__PODCAST_JSON_FILE = DATA_DIR / PODCAST_INDEX_DIR / PODCAST_JSON_PREFIX / '114514_abcdedfdsf.json'
__DEMO__PODCAST_AUDIO_FILE = DATA_DIR / PODCAST_AUDIO_DIR / '114514/guid_sha1_aabbcc/ep123.mp3'
LOCK_FILE = 'preserve_podcasts.lock'

 # title mark
TITLE_MARK_PREFIX = '_=TITLE=='
MARKS_SUFFIX = '.mark'

EPISODE_DOWNLOAD_CHUNK_SIZE = 1024 * 337 # bytes

REFRESH_INTERVAL = 60 * 60 * 24 # 24 hours


def checkFeedSize(data: bytes):
    if data is None:
        return
    if len(data) > FEED_SIZE_LIMIT:
        raise FeedTooLargeError('Feed too large')


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
def download_episode(session: requests.Session, url: str, *, guid: str, episode_dir: Path, filename: str,
                    possible_size: int=-1, title: str= '',
                    force_redownload: bool = False):
    to_download = True
    possible_sizes = [possible_size]

    ep_audio_file_path = episode_dir / filename
    ep_audio_meta_path = episode_dir / (filename + '.metadata.json')
    if ep_audio_meta_path.exists():
        with open(ep_audio_meta_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            possible_sizes.append(metadata['http-content-length']) if 'http-content-length' in metadata else None

    if os.path.exists(ep_audio_file_path) and os.path.getsize(ep_audio_file_path) in possible_sizes:
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

        # content_disposition = get_content_disposition(r)
        suggested_filename = get_suggested_filename(r)

        print(f'content-length: {content_length}, etag: [green]{etag}[/green], last-modified: [yellow]{last_modified}[/yellow], suggested_filename: [blue]{suggested_filename}[/blue]')
        if content_length > 0 and content_length != possible_size:
            if os.path.exists(ep_audio_file_path) and os.path.getsize(ep_audio_file_path) == content_length:
                print('File already exists')
                possible_sizes.append(content_length) if content_length > 0 and content_length not in possible_sizes else None
                to_download = False

        real_size = 0
        if to_download or force_redownload:
            os.makedirs(os.path.dirname(ep_audio_file_path), exist_ok=True)
            with open(ep_audio_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=EPISODE_DOWNLOAD_CHUNK_SIZE):
                    real_size += len(chunk)
                    checkEpisodeAudioSize(real_size, [possible_size, content_length])
                    f.write(chunk)
            print('') # new line

            # create title mark file
            safe_title = safe_chars(title)
            if safe_title and not os.path.exists(os.path.join(episode_dir, TITLE_MARK_PREFIX+safe_title+MARKS_SUFFIX)):
                with open(os.path.join(episode_dir, TITLE_MARK_PREFIX+safe_title+MARKS_SUFFIX),
                          'w', encoding='utf-8') as f:
                    if type(title) == str and title:
                        f.write(title)
                    else:
                        f.write('')
            
            time.sleep(3)

            print('\nAudio duration:', audio_duration(ep_audio_file_path))

            save_audio_file_metadata(
                audio_path=ep_audio_file_path, metadata_path=ep_audio_meta_path, r=r,
                renew=True)

        # modify file modification time
        if last_modified:
            atime = os.path.getatime(ep_audio_file_path) # keep access time
            mtime = float_last_modified(last_modified)
            if mtime:
                os.utime(ep_audio_file_path, (atime, mtime)) # modify file update time
            else:
                print('mtime error:', mtime)


def save_entry(entry:dict, file_path:str):
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(entry, indent=4, ensure_ascii=False))


@runtimeTypeCheck()
def save_podcast_index_json(podcast: Podcast, podcast_json_file_path: Optional[Path] = None):
    if podcast.get('id') is None:
        raise ValueError('No id')

    if podcast_json_file_path is None:
        podcast_json_file_path = DATA_DIR / PODCAST_INDEX_DIR / f'{PODCAST_JSON_PREFIX}{podcast.id}_{podcast.title[:30]}.json'
        logger.debug(f'new podcast_json_file_path: {podcast_json_file_path}')

    with open(podcast_json_file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(podcast.to_dict(), indent=4, ensure_ascii=False))

def save_audio_file_metadata(
        audio_path: Path, metadata_path: Path, r: requests.Response,
        renew: bool = False
        ):
    ''' renew: re calculate sha1, md5 '''
    content_length = get_content_length(r)

    if not renew and os.path.exists(metadata_path):
        with open(metadata_path, 'r', encoding='utf-8') as f:
            old_metadata = json.load(f)
    else:
        old_metadata = {}
    
    url_history = {}
    for i, redirect in enumerate(r.history):

        # del 'Set-Cookie'
        _headers = dict(redirect.headers)
        for set_cookie in ['Set-Cookie', 'set-cookie']:
            if set_cookie in _headers:
                del _headers[set_cookie]

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
    with open(metadata_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(metadata, indent=4, ensure_ascii=False))


def lowercase_headers(headers: CaseInsensitiveDict) -> Dict:
    return {k.lower(): v for k, v in headers.items()}


def do_archive(podcast: Podcast, session: requests.Session, delete_episodes_not_in_feed: bool = False):
    try:
        r = session.get(podcast.feed_url, headers={'User-Agent': PRESERVE_THOSE_POD_UA})
        
        d: feedparser.FeedParserDict = feedparser.parse(r.content,
            response_headers = lowercase_headers(r.headers), request_headers=r.request.headers,
            agent = PRESERVE_THOSE_POD_UA,
            sanitize_html = True,
            resolve_relative_uris = True
            )
        # d: feedparser.FeedParserDict = feedparser.parse(podcast.feed_url)

        if d.get('bozo_exception', None) is not None:
            if len(d.feed) == 0:
                raise d.bozo_exception # type: ignore
            logger.warn(f'bozo_exception: {d.bozo_exception}')

        podcast.load(d.feed) # type: ignore @runtimeTypeCheck
    except Exception as e:
        podcast.update_failed()
        raise e

    if DEBUG_MODE:
        os.makedirs('debug', exist_ok=True)
        with open(f'debug/{podcast.id}_{int(time.time())}.debug.json', 'w', encoding='utf-8') as f:
            f.write(json.dumps(d, indent=4, ensure_ascii=False))

    podcast_audio_dir = DATA_DIR / PODCAST_AUDIO_DIR / podcast.id

    archive_entries(d=d, session=session, podcast_audio_dir=podcast_audio_dir,
                    delete_episodes_not_in_feed=delete_episodes_not_in_feed)

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


def archive_entries(d: feedparser.FeedParserDict, session: requests.Session, podcast_audio_dir: Path,
                    delete_episodes_not_in_feed: bool = False):
    sha1ed_guids = set()

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
        image_href: Optional[str] = entry.get('image', {}).get('href') # type: ignore
        print(f'Image href: {image_href}')

        itunes_title = entry.get('itunes_title')
        itunes_duration = entry.get('itunes_duration')
        itunes_season = entry.get('itunes_season')
        itunes_episode = entry.get('itunes_episode')
        print(f'itunes_title: [yellow]{itunes_title}[/yellow]')
        print(
            '\t'.join(
                [
                    f'itunes_duration: {itunes_duration}',
                    f'itunes_season: {itunes_season}',
                    f'itunes_episode: {itunes_episode}'
                ]
            )
        )

        for link in entry.links:
            # The enclosure must have three attributes: url, length, and type.
            if not link.has_key('href') or not link.has_key('length') or not link.has_key('type'):
                continue
            if 'audio' not in link.type: # only download audio
                continue

            print(link.href)
            print(link.type)

            # According to the best practice <https://www.rssboard.org/rss-profile#element-channel-item-enclosure>,
            # When an enclosure's size cannot be determined, a publisher should use a length of 0.
            # But in realworld, some podcast feed use "None" or "unknown" to represent the length is unknown.
            length = link.get('length', -1) # use -1 as unknown length (magic number)
            try:
                length = int(length) # type: ignore
                if length <= 0:
                    logger.warn(f'link.length: {length} <= 0')
                    length = -1
            except ValueError:
                logger.warn(f'Unable to int(length), length is "{length}"')
                length = -1
            print(length, "(", int(length/1024/1024), "MiB )") # type: ignore

            sha1ed_guid = sha1(guid.encode('utf-8'))
            sha1ed_guids.add(sha1ed_guid)

            episode_dir = podcast_audio_dir / sha1ed_guid

            download_episode(session, link.href, possible_size=length, guid=guid, # type: ignore
                                episode_dir=episode_dir,
                                filename=url2audio_filename(link.href), # type: ignore @runtimeTypeCheck
                                title=title,
            )
            save_entry(entry, file_path=os.path.join(episode_dir, f'entry_guid_sha1_{sha1ed_guid}.json'))
            break # avoid downloading multiple audio files

    if delete_episodes_not_in_feed:
        # delete episodes not in feed
        local_episode_dirs = set(os.listdir(podcast_audio_dir))

        logger.debug(f'local_episode_dirs: {local_episode_dirs}')
        logger.debug(f'sha1ed_guids: {sha1ed_guids}')

        episodes_not_in_feed_dirs = local_episode_dirs ^ sha1ed_guids

        logger.debug(f'episodes_not_in_feed_dirs: {episodes_not_in_feed_dirs}')

        for dir in episodes_not_in_feed_dirs:
            print(f'[red]Episode not in feed, deleting {dir}[/red]')
            shutil.rmtree(os.path.join(podcast_audio_dir, dir))


def all_podcast_id(use_cache: bool=False)-> Set[str]:
    ''' return a set of all feed url sha1.

    Note: URL ends with `/` will be `rstrip('/')` before hashing
    Note: `http://` URL will be treated as the same `https://` URL before hashing
    '''
    if use_cache:
        with open(DATA_DIR / PODCAST_INDEX_DIR / PODCAST_ID_CACHE, 'r', encoding='utf-8') as f:
            id_list = f.read().splitlines()
            return set(id_list)

    podcast_id_set = set()
    podcast_index_dirs = os.listdir(DATA_DIR / PODCAST_INDEX_DIR) if os.path.exists(DATA_DIR / PODCAST_INDEX_DIR) else []
    for podcast_idnex_dir in podcast_index_dirs:
        if podcast_idnex_dir.startswith(PODCAST_JSON_PREFIX):
            podcast_id = podcast_idnex_dir[len(PODCAST_JSON_PREFIX):].split('_')[0]
            podcast_json_file_path = DATA_DIR / PODCAST_INDEX_DIR / podcast_idnex_dir
            with open(podcast_json_file_path, 'r', encoding='utf-8') as f:
                podcast_json = json.load(f)
            feed_url: str = podcast_json['feed_url']
            assert podcast_guid_uuid5(feed_url) == podcast_id
            podcast_id_set.add(podcast_id)
    with open(DATA_DIR / PODCAST_INDEX_DIR / PODCAST_ID_CACHE, 'w', encoding='utf-8') as f:
        # separators=(',\n', ': '))
        f.write('\n'.join(podcast_id_set))
        print(f'all_feed_url_sha1 cache refreshed/loaded: {len(podcast_id_set)}')

    return podcast_id_set



def add_podcast(session: requests.Session, feed_url: str):
    print(f'Adding podcast: {feed_url}')
    if podcast_guid_uuid5(feed_url) in all_podcast_id():
        raise ValueError(f'Podcast already exists (guid: "{podcast_guid_uuid5(feed_url)}")\n')

    this_podcast = Podcast()
    this_podcast.create(init_feed_url=feed_url)
    print(f'Podcast id: {this_podcast.id}')
    with FileLock(DATA_DIR / PODCAST_LOCK_DIR, this_podcast.id):
        do_archive(this_podcast, session=session, delete_episodes_not_in_feed=True)
    save_podcast_index_json(this_podcast)
    all_podcast_id()


def get_args():
    import argparse
    parser = argparse.ArgumentParser()
    # parser.add_argument('--debug', action='store_true')
    parser.add_argument('-a','--add', nargs='+', help='RSS feed URL(s)', default=[])
    parser.add_argument('-u','--update', action='store_true', help='Update podcasts')
    parser.add_argument('--only', nargs='+', help='[dev] Only update these podcast ids', default=[])
    parser.add_argument("--insecure", action='store_true', help="Disable SSL certificate verification")

    args = parser.parse_args()
    if args.update and args.add:
        parser.error('--update can not be used with RSS feed URL(s)')
    if args.only:
        raise NotImplementedError('--only')
    return args

def get_podcast_json_file_paths():
    for podcast_json_file_path in (DATA_DIR / PODCAST_INDEX_DIR).glob(f'{PODCAST_JSON_PREFIX}*.json'):
        yield podcast_json_file_path

def update_all(session: requests.Session):
    for podcast_json_file_path in get_podcast_json_file_paths():
        this_podcast = Podcast()
        this_podcast.load(podcast_json_file_path)
        assert this_podcast.id

        if this_podcast.enabled is False:
            print(f'Podcast {this_podcast.id}: {this_podcast.title} is disabled')
            continue
        if (time.time() - this_podcast.saveweb['last_success_timestamp']) < REFRESH_INTERVAL:
            print(f'Podcast {this_podcast.id}: {this_podcast.title} not need to update')
            continue

        print(f'Podcast {this_podcast.id}: {this_podcast.title} updating...')
        try:
            with FileLock(DATA_DIR / PODCAST_LOCK_DIR, this_podcast.id):
                do_archive(this_podcast, session=session)
        except AlreadyRunningError:
            print("Another instance is archiving this podcast, skip.")
            continue
        save_podcast_index_json(this_podcast, podcast_json_file_path=Path(podcast_json_file_path))


def main():
    session = create_session()
    args = get_args()

    (DATA_DIR / PODCAST_INDEX_DIR).mkdir(parents=True, exist_ok=True)
    (DATA_DIR / PODCAST_LOCK_DIR).mkdir(parents=True, exist_ok=True)
    (DATA_DIR / PODCAST_AUDIO_DIR).mkdir(parents=True, exist_ok=True)

    if args.insecure:
        session.verify = False
        requests.packages.urllib3.disable_warnings() # type: ignore
        logger.warning("SSL certificate verification disabled")

    for feed_url in args.add:
        try:
            add_podcast(session, feed_url)
        except ValueError as e:
            if str(e).startswith('Podcast already exists'):
                print(str(e))
            else:
                raise e

    if args.update:
        update_all(session=session)


if __name__ == '__main__':
    main()