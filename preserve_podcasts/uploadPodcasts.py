import argparse
from dataclasses import dataclass
import datetime
import io
import json
import logging
from pathlib import Path
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse
import requests

from rich import print
from internetarchive import get_item, Item
from preserve_podcasts.pod_sessiosn import PRESERVE_THOSE_POD_UA

from preserve_podcasts.utils.util import sha1
from preserve_podcasts.podcast import Podcast
from preserve_podcasts.preservePodcasts import get_podcast_json_file_paths
from preserve_podcasts.preservePodcasts import (
    DATA_DIR, PODCAST_INDEX_DIR, PODCAST_AUDIO_DIR, PODCAST_JSON_PREFIX,
    PODCAST_ID_CACHE, TITLE_MARK_PREFIX, MARKS_SUFFIX,
)
MARKS_PREFIX = "_"
UPLOADED_MARK = "_uploaded.mark"

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
    args = parser.parse_args()

    return Args(**vars(args))

def upload_podcasts(args: Args):
    for podcast_json_file_path in get_podcast_json_file_paths():
        this_podcast = Podcast()
        this_podcast.load(podcast_json_file_path)
        assert this_podcast.id

        # if not this_podcast.enabled
        #     continue

        upload_podcast(this_podcast, args=args)
        

def upload_podcast(podcast: Podcast, args: Args):
    logger.info(f'Uploading podcast: {podcast.id}: {podcast.title}')
    podcast_audio_dir = DATA_DIR / PODCAST_AUDIO_DIR / podcast.id
    for ep_audio_dir in podcast_audio_dir.iterdir():
        if not ep_audio_dir.is_dir():
            logger.warn(f'Not a directory: {ep_audio_dir}')
            continue
        if (ep_audio_dir / UPLOADED_MARK).exists():
            logger.debug(f'Already uploaded: {ep_audio_dir}')
            continue
        upload_episode(podcast, ep_audio_dir, args=args)

def find_ep_metadata_file(files: list[Path])->Tuple[Optional[Path], Optional[str]]:
    for file in files:
        if file.name.startswith("entry_guid_sha1_") and file.suffix == ".json":
            return (file, file.name[len("entry_guid_sha1_"):-len(".json")])

    return (None, None)

def best_description(ep_metadata: dict)->str:
    description = None
    if 'content' in ep_metadata:
        description = ep_metadata['content'][0]['value']
    elif 'summary' in ep_metadata:
        description = ep_metadata['summary']
    else:
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

    image_name = f"{ep_sha1ed_guid}_itemimage.{suffix}"
    ia_keys = IAKeys(args.keys_file)
    r = item.upload({image_name : image_io},
                    access_key=ia_keys.access,
                    secret_key=ia_keys.secret,)
    logger.debug(f"Upload image response: {r}")

def upload_episode(podcast: Podcast, ep_audio_dir: Path, args: Args):
    logger.info(f'Uploading episode: {ep_audio_dir}')
    files = list(ep_audio_dir.glob('*'))

    filedict = {}
    for file in files:
        if file.name.startswith(MARKS_PREFIX) and file.name.endswith(MARKS_SUFFIX):
            logger.debug(f'Found title mark file: {file}')
            continue

        filedict[file.name] = file

    print(filedict)

    ep_metadata_file, ep_sha1ed_guid = find_ep_metadata_file(files)
    if ep_metadata_file is None or ep_sha1ed_guid is None:
        logger.warn(f'No metadata file found: {ep_audio_dir}, skipping')
        return

    ep_metadata = json.loads(ep_metadata_file.read_text(encoding='utf-8'))

    assert sha1(ep_metadata['id']) == ep_sha1ed_guid, f"sha1({ep_metadata['id']}) != {ep_sha1ed_guid}"

    identifier = f"podcast_ep_{ep_sha1ed_guid}"
    print(f"Identifier: {identifier}")
    item = get_item(identifier)

    for file_in_item in item.files:
        if file_in_item["name"] in filedict:
            filedict.pop(file_in_item["name"])
            print(f"File {file_in_item['name']} already exists in {identifier}.")
    print(f"Uploading {len(filedict)} files...")


    keywords = ["Podcast", "Podcasts"]
    keywords += [podcast.title]
    if podcast.tags:
        for tag in podcast.tags:
            keywords.append(tag['term'])
    logger.debug(f'Keywords: {keywords}')

    description = best_description(ep_metadata)

    published_parsed :List = ep_metadata['published_parsed']
    # [2022, 1, 29, 8, 4, 49, 5, 29, 0]
    date =  datetime.datetime(*published_parsed[:6]).isoformat()

    external_identifier = []
    # https://podcastindex.org/namespace/1.0
    external_identifier.append(f"urn:podcast:guid:{podcast.id}")
    external_identifier.append(f"urm:rss:channel:item:guid:{ep_metadata['id']}")

    metadata_init = {
        "mediatype": "audio",
        "collection": args.collection,

        "subject": "; ".join(
            keywords
        ),  # Keywords should be separated by ; but it doesn't matter much; the alternative is to set one per field with subject[0], subject[1], ...

        "title": ep_metadata['title'],
        "subtitle": ep_metadata.get('subtitle'),
        "description": description,
        "guid": ep_metadata['id'], # ep_guid, as same as LifePod-Beta
        # ep_sha1ed_guid == sha1(ep_metadata['id'])
        "guid_sha1": ep_sha1ed_guid, # so people won't get confused with the item identifier
        "link": ep_metadata['link'],
        "date": date,
        "creator": [podcast.title, ep_metadata['author']], # podcast.author may contain author's Email address, DO NOT USE

        "podcast_title": podcast.title,
        "podcast_feedurl": podcast.feed_url,
        "podcast_subtitle": podcast.get('subtitle'),
        "podcast_summary": podcast.get('summary'),
        "podcast_link": podcast.link,

        "language": podcast.get('language'),
        
        "external-identifier": external_identifier,

        "upload-state": "uploading",
        "scanner": PRESERVE_THOSE_POD_UA,
    }

    print(metadata_init)

    ia_keys = IAKeys(args.keys_file)

    if args.dry_run:
        print(best_image_href(podcast, ep_metadata))
        print("Dry run, skipping upload")
        return

    try:
        r = item.upload(files=filedict, metadata=metadata_init,
                access_key=ia_keys.access,
                secret_key=ia_keys.secret,
                verbose=True,
                queue_derive=True,
                retries=10,
            )
    except requests.exceptions.HTTPError as e:
        if "appears to be spam." in str(e):
            if args.debug:
                raise e
            logger.error(f"Upload failed: appears to be spam: {e}")
            return

    tries = 700
    item = get_item(identifier)  # refresh item
    while not item.exists and tries > 0:
        print(f"Waiting for item to be created ({tries})  ...", end="\r")
        time.sleep(30)
        try:
            item = get_item(identifier)
        except Exception as e:
            print("Failed to get item:", e, end="(retrying)")
        tries -= 1

    assert item.exists, "Item not created"

    if image_href := best_image_href(podcast, ep_metadata):
        print(f"Uploading item image... (optional)")
        upload_itemimage(ep_sha1ed_guid, image_href, item, args=args)

    item.modify_metadata({"upload-state": "uploaded"})

    with open(ep_audio_dir / UPLOADED_MARK, "w", encoding="utf-8") as f:
        f.write(f"Uploaded to {identifier} at {datetime.datetime.now().isoformat()}")

    print(f"==> Uploaded {identifier} successfully!")
    print(f"==> https://archive.org/details/{identifier}")


def main():
    args = get_args()
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    upload_podcasts(args=args)


if __name__ == '__main__':
    main()