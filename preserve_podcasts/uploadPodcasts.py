import argparse
from dataclasses import dataclass
import datetime
import io
import json
import logging
import os
from pathlib import Path
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests
from rich import print
from internetarchive import get_item, Item, get_session, ArchiveSession
from preserve_podcasts.pod_sessiosn import PRESERVE_THOSE_POD_UA
from preserve_podcasts.utils.fileLock import AlreadyRunningError, FileLock
from preserve_podcasts.utils.requests_patch import SessionMonkeyPatch

from preserve_podcasts.utils.util import sha1
from preserve_podcasts.podcast import Podcast
from preserve_podcasts.preservePodcasts import get_podcast_json_file_paths
from preserve_podcasts.preservePodcasts import (
    DATA_DIR, PODCAST_INDEX_DIR, PODCAST_AUDIO_DIR, PODCAST_JSON_PREFIX,
    PODCAST_ID_CACHE, TITLE_MARK_PREFIX, MARKS_SUFFIX,
)

EPISODE_LOCK_DIR = "episode_lock/"
MARKS_PREFIX = "_"
PENDING_MARK = "_pending.mark"
UPLOADED_MARK = "_uploaded.mark"
SPAM_MARK = "_spam.mark"

logger = logging.Logger(__name__)

@dataclass
class IAKeys:
    access: str
    secret: str

    def __init__(self, path: Path):
        with open(path.expanduser().resolve()) as f:
            lines = f.readlines()

        self.access = lines[0].strip()
        self.secret = lines[1].strip()

@dataclass
class Args:
    keys_file: Path
    collection: str
    dry_run: bool
    debug: bool
    not_spam: bool = False
    no_wait: bool = False
    insecure: bool = False

    def __post_init__(self):
        self.keys_file = Path(self.keys_file).expanduser().resolve()
        if not self.keys_file.exists():
            raise FileNotFoundError(f"Keys file {self.keys_file} does not exist")

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-kf", "--keys_file", default="~/.pod_ia_keys",
                        help="Path to file containing IA keys [default: ~/.pod_ia_keys]")
    parser.add_argument("-c", "--collection", default="opensource_audio", help="IA collection",
                        choices=["opensource_audio", "test_collection"])
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--debug", action="store_true", help="Debug")
    parser.add_argument("--not-spam", action="store_true", help="Re-upload episodes marked as spam by IA previously")
    parser.add_argument("--no-wait", action="store_true", help="Don't wait for item to be created") # upload full metadata initially
    parser.add_argument("--insecure", action="store_true", help="Don't verify SSL certificate")
    args = parser.parse_args()

    return Args(**vars(args))

def upload_podcasts(args: Args, session: ArchiveSession):
    for podcast_json_file_path in get_podcast_json_file_paths():
        this_podcast = Podcast()
        this_podcast.load(podcast_json_file_path)
        assert this_podcast.id

        # if not this_podcast.enabled
        #     continue

        upload_podcast(this_podcast, args=args, session=session)
        

def upload_podcast(podcast: Podcast, args: Args, session: ArchiveSession):
    logger.info(f'Uploading podcast: {podcast.id}: {podcast.title}')
    podcast_audio_dir = DATA_DIR / PODCAST_AUDIO_DIR / podcast.id
    for ep_audio_dir in podcast_audio_dir.iterdir():
        if not ep_audio_dir.is_dir():
            logger.warn(f'Not a directory: {ep_audio_dir}')
            continue
        if (ep_audio_dir / UPLOADED_MARK).exists():
            logger.info(f'Already uploaded: {ep_audio_dir}')
            continue
        if (ep_audio_dir / SPAM_MARK).exists() and not args.not_spam:
            logger.warn(f'Marked as spam by IA: {ep_audio_dir}, skipping. (use --not-spam to reupload)')
            continue
        try:
            with FileLock(DATA_DIR / EPISODE_LOCK_DIR, ep_audio_dir.name):
                upload_episode(podcast, ep_audio_dir, args=args, session=session)
        except AlreadyRunningError:
            logger.warn(f"Another instance is uploading {ep_audio_dir.name}, skipping.")
            continue

def find_ep_metadata_file(files: list[Path])->Tuple[Optional[Path], Optional[str]]:
    for file in files:
        if file.name.startswith("entry_guid_sha1_") and file.suffix == ".json":
            return (file, file.name[len("entry_guid_sha1_"):-len(".json")])

    return (None, None)

def best_description(ep_metadata: dict, strict: bool=False)->Optional[str]:
    description: Optional[str] = None
    if 'content' in ep_metadata:
        description = ep_metadata['content'][0]['value']
    elif 'summary' in ep_metadata:
        description = ep_metadata['summary']
    else:
        logger.warn(f'No description found')
        if strict:
            raise Exception(f'No description found: {ep_metadata}')
    
    return description


def best_image_href(podcast: Podcast, ep_metadata: dict)->Optional[str]:
    image_href = None
    if 'image' in ep_metadata:
        logger.debug(f'Found image in ep_metadata: {ep_metadata}')
        image_href = ep_metadata['image']['href']
    elif podcast.image:
        logger.debug(f'Using podcast image: {podcast.image["href"]}')
        image_href = podcast.image['href']
    else:
        logger.warn(f'No image found: {ep_metadata}')

    return image_href


def upload_itemimage(ep_sha1ed_guid: str, image_href: str, item: Item, args: Args):
    suffix = urlparse(image_href).path.split(".")[-1]
    suffix = suffix.lower()
    if suffix not in ["jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "webp", "svg", "ico"]:
        logger.warn(f"Unknown image suffix: {suffix}")
        suffix = "jpg"
    image_name = f"{ep_sha1ed_guid}_itemimage.{suffix}"
    for file in item.files:
        if file["name"] == image_name:
            logger.info(f"Image already exists: {image_name}")
            return

    session = item.session
    image_io = None
    for i in range(3):
        try:
            r = session.get(image_href)
            r.raise_for_status()
            image_io = io.BytesIO(r.content)
            break
        except Exception as e:
            logger.warn(f'Failed to download image: {image_href}: {e}, retrying({i})')
            continue
    if image_io is None:
        logger.warn(f'Failed to download image: {image_href}, skipping image upload')
        return

    ia_keys = IAKeys(args.keys_file)
    r = item.upload({image_name : image_io},
                    access_key=ia_keys.access,
                    secret_key=ia_keys.secret,)
    logger.debug(f"Upload image response: {r}")


def wait_for_item(identifier: str, session: ArchiveSession):
    """ infinite wait for item to be created """
    item = get_item(identifier, archive_session=session)  # refresh item
    waited = 0
    while not item.exists:
        print(f"Waiting for item to be created ({waited}s waited)...", end="\r")
        time.sleep(30)
        waited += 30
        try:
            item = get_item(identifier, archive_session=session)
        except Exception as e:
            print("Failed to get item:", e, end="(retrying)")
    
    return item

def sort_files_by_size(files: list[Path], ascending: bool = True)->list[Path]:
    return sorted(files, key=lambda f: f.stat().st_size, reverse=not ascending)


def pop_uploaded_files(filedict: dict[str, Path], item: Item):
    if not item.exists:
        logger.warn(f"Item {item.identifier} does not exist, no need to pop uploaded files")
        return
    for file_in_item in item.files:
        if file_in_item["name"] in filedict:
            filedict.pop(file_in_item["name"])
            print(f"File {file_in_item['name']} already exists in {item.identifier}.")


def upload_episode(podcast: Podcast, ep_audio_dir: Path, args: Args, session: ArchiveSession):
    logger.info(f'Uploading episode: {ep_audio_dir}')
    files = list(ep_audio_dir.glob('*'))

    assert len([file for file in files if file.is_file()]) == len(files), 'Some "file(s)" is not file'

    files = sort_files_by_size(files, ascending=True) # small files first, speed up IA's item creation

    ep_metadata_file, ep_sha1ed_guid = find_ep_metadata_file(files)
    if ep_metadata_file is None or ep_sha1ed_guid is None:
        logger.warn(f'No metadata file found: {ep_audio_dir}, skipping. (probably a incomplete download)')
        return "No metadata file found"

    ep_metadata = json.loads(ep_metadata_file.read_text(encoding='utf-8'))

    assert sha1(ep_metadata['id']) == ep_sha1ed_guid, f"sha1({ep_metadata['id']}) != {ep_sha1ed_guid}"

    filedict = {}
    for file in files:
        if file.name.startswith(MARKS_PREFIX) and file.name.endswith(MARKS_SUFFIX):
            logger.debug(f'Found title mark file: {file}')
            continue

        filedict[file.name] = file
        print(file.name, "<==", str(file))


    identifier = f"podcast_ep_{ep_sha1ed_guid}"
    print(f'Identifier: "{identifier}"')

    keywords = ["Podcast", "Podcasts"]
    keywords += [podcast.title]
    if podcast.tags:
        for tag in podcast.tags:
            keywords.append(tag['term'])
    logger.debug(f'Keywords: {keywords}')

    description = best_description(ep_metadata)

    published_parsed :List = ep_metadata['published_parsed']
    # [2022, 1, 29, 8, 4, 49, 5, 29, 0]
    date =  datetime.datetime(*published_parsed[:6]).strftime("%Y-%m-%d %H:%M:%S")

    external_identifier = []
    # https://podcastindex.org/namespace/1.0#Guid
    external_identifier.append(f"urn:podcast:guid:{podcast.id}")
    # https://www.rssboard.org/rss-specification#ltguidgtSubelementOfLtitemgt
    external_identifier.append(f"urn:rss:channel:item:guid:{ep_metadata['id']}")
    # TODO: spotify, apple, google, podcastindex_id, etc.

    metadata_init = {
        "mediatype": "audio",
        "collection": args.collection,

        "subject": "; ".join(
            keywords
        ),  # Keywords should be separated by ; but it doesn't matter much; the alternative is to set one per field with subject[0], subject[1], ...

        "title": ep_metadata['title'],
        "subtitle": ep_metadata.get('subtitle'),
        "description": f"{ep_sha1ed_guid} uploading...",
        "guid": ep_metadata['id'], # ep_guid, as same as LifePod-Beta
        # ep_sha1ed_guid == sha1(ep_metadata['id'])
        "guid_sha1": ep_sha1ed_guid, # so people won't get confused with the item identifier
        "link": ep_metadata['link'],
        "date": date,
        "creator": [podcast.title, ep_metadata['author']], # podcast.author may contain author's Email address, DO NOT USE

        "podcast_title": podcast.title,
        "podcast_feedurl": podcast.feed_url,
        "podcast_link": podcast.link,
        "podcast_subtitle": podcast.get('subtitle'),
        "podcast_summary": podcast.get('summary'),

        # TODO: itunes...

        "language": podcast.get('language'),
        
        "external-identifier": external_identifier,

        "upload-state": "unknown" if args.no_wait else "uploading",
        "scanner": PRESERVE_THOSE_POD_UA,
    }


    # with open(identifier + "_metadata.json", "w", encoding="utf-8") as f:
    #     json.dump(metadata_init, f, indent=4, ensure_ascii=False)

    if args.dry_run:
        print(metadata_init)
        print(best_image_href(podcast, ep_metadata))
        print("Dry run, skipping upload")
        return

    if not (ep_audio_dir / PENDING_MARK).exists():
        # fresh upload
        logger.debug("No pending mark found, this is a fresh upload")

        logger.debug("Getting item...")
        item = get_item(identifier, archive_session=session)

        if item.exists:
            if item.metadata.get("upload-state","") == "uploaded":
                logger.info(f"Item {identifier} already exists, skipping")
                mark_as_uploaded(ep_audio_dir, identifier)
                return True

            pop_uploaded_files(filedict, item)

        print(f"Uploading {len(filedict)} files...")

        ia_keys = IAKeys(args.keys_file)
        try:
            print(metadata_init)
            r = item.upload(files=filedict, metadata=metadata_init,
                    access_key=ia_keys.access,
                    secret_key=ia_keys.secret,
                    verbose=True,
                    queue_derive=True,
                    retries=10,
                )
        except requests.exceptions.HTTPError as e:
            if "appears to be spam." in str(e):
                with open(ep_audio_dir / SPAM_MARK, "w", encoding="utf-8") as f:
                    f.write(f"Spam")
                logger.error(f"Upload failed: appears to be spam: {e}")
                return "Upload failed: appears to be spam"

        with open(ep_audio_dir / PENDING_MARK, "w", encoding="utf-8") as f:
            f.write(f"Pending {identifier} to be created...")
    else: # pending previously
        logger.info("Found pending mark")

    # fresh uploaded or item created from pending

    item = wait_for_item(identifier, session) if not args.no_wait else get_item(identifier, archive_session=session)

    # NOTE:
    # if args.no_wait:
        # item.exists is Unkown(T/F)
    # else: # wait_for_item
        # item.exists is True

    if image_href := best_image_href(podcast, ep_metadata):
        print(f"Uploading item image... (optional)")
        upload_itemimage(ep_sha1ed_guid, image_href, item, args=args)

    if args.no_wait:
        if not item.exists:
            logger.warn(f"Item {identifier} still in queue (pending), skipping. (pls re-run this script later))")
            return "Still in queue, skipped"

    assert item.exists

    new_metadata = {}
    if item.metadata["upload-state"] != "uploaded":
        new_metadata["upload-state"] = "uploaded"
    if description and item.metadata["description"] != description:
        new_metadata["description"] = description
    if item.metadata["external-identifier"] != external_identifier:
        new_metadata["external-identifier"] = external_identifier

    if new_metadata:
        print(f"Updating metadata...")
        print(new_metadata)
        ia_keys = IAKeys(args.keys_file)
        r = item.modify_metadata(
            metadata=new_metadata,
            access_key=ia_keys.access,
            secret_key=ia_keys.secret
        )
        assert isinstance(r, requests.Response)
        r.raise_for_status()
        print(r.text)
        assert r.json()["success"] == True
    
    mark_as_uploaded(ep_audio_dir, identifier)
    return True


def mark_as_uploaded(ep_audio_dir: Path, identifier: str):
    with open(ep_audio_dir / UPLOADED_MARK, "w", encoding="utf-8") as f:
        f.write(f"Uploaded to {identifier} at {datetime.datetime.now().isoformat()}")

    if (ep_audio_dir / SPAM_MARK).exists():
        os.remove(ep_audio_dir / SPAM_MARK)
    if (ep_audio_dir / PENDING_MARK).exists():
        os.remove(ep_audio_dir / PENDING_MARK)

    print(f"==> Uploaded {identifier} successfully!")
    print(f"==> https://archive.org/details/{identifier}")

def main():
    args = get_args()

    session: ArchiveSession = get_session()
    if args.insecure:
        session.verify = False
        requests.packages.urllib3.disable_warnings() # type: ignore
        logger.warning("SSL certificate verification disabled")

    sess_patcher = SessionMonkeyPatch(session=session)
    sess_patcher.hijack()

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    if args.debug:
        logger.setLevel(logging.DEBUG)

        def print_request(r: requests.Response, *args, **kwargs):
        # TODO: use logging
        # print("H:", r.request.headers)
            print(f"Resp: {r.request.method} {r.status_code} {r.reason} {r.url}")
            if r.raw._connection.sock:
                print(f"Conn: {r.raw._connection.sock.getsockname()} -> {r.raw._connection.sock.getpeername()[0]}")
        session.hooks['response'].append(print_request)
    else:
        logger.setLevel(logging.INFO)

    
    upload_podcasts(args=args, session=session)


if __name__ == '__main__':
    main()