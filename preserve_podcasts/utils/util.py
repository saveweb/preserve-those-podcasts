import hashlib
import logging
from typing import Union
from urllib.parse import urlparse
import uuid

from preserve_podcasts.utils.type_check import runtimeTypeCheck


NTFS_CHARS = r'<>:"/\|?*'
UNPRINTABLE_CHARS = ''.join(chr(i) for i in range(32))
NAMESPACE_PODCAST = uuid.UUID("ead4c236-bf58-58c6-a2c6-a6b28d128cb6")

logger = logging.Logger(__name__)

@runtimeTypeCheck()
def remove_unprintable_chars(s: str) -> str:
    """Remove unprintable characters."""
    string = ''.join(c if c not in UNPRINTABLE_CHARS else '' for c in s)

    return string


@runtimeTypeCheck()
def replace_ntfs_chars(s: str, replace_space: bool=True) -> str:
    """Replace NTFS reserved characters with underscores."""
    to_replace = NTFS_CHARS
    if replace_space:
        to_replace += ' '
    string = ''.join(c if c not in to_replace else '_' for c in s)

    return string

@runtimeTypeCheck()
def safe_chars(s: str, replace_space: bool=True, max_bytes: int=240, replace_last_dot: bool=True) -> str:
    string = remove_unprintable_chars(s)
    string = replace_ntfs_chars(string, replace_space=replace_space)

    while len(string.encode('utf-8')) > max_bytes:
        string = string[:-1]
    
    if replace_last_dot and string.endswith('.'):
        string = string[:-1] + '_'

    return string

def sha1(data: Union[bytes,str]):
    sha1 = hashlib.sha1()
    if isinstance(data, str):
        sha1.update(data.encode('utf-8'))
    else:
        sha1.update(data)
    return sha1.hexdigest()


def podcast_guid_uuid5(feed_url: str) -> str:
    ''' return the uuid5 (with `-`) of the feed url

    <https://podcastindex.org/namespace/1.0#guid>
    Note: protocol scheme and trailing slashes stripped off
    '''
    assert feed_url.startswith(('http://', 'https://', '//'))

    stripped_url = feed_url.lstrip('http://').lstrip('https://').lstrip('//').rstrip('/')

    return str(uuid.uuid5(NAMESPACE_PODCAST, stripped_url))
