from typing import Optional, Union
import time

import requests
import pyrfc6266
from rich import print

from preserve_podcasts.utils.type_check import runtimeTypeCheck

def get_content_length(r: requests.Response) -> int:
    """Get the `content-length` header from a response.

    If the header is not present, return -1.
    """
    return int(r.headers.get('content-length', -1))


def get_content_type(r: requests.Response) -> Optional[str]:
    """Get the `content-type` header from a response.

    If the header is not present, return None.
    """
    return r.headers.get('content-type', None)


def get_etag(r: requests.Response) -> Optional[str]:
    """Get the `etag` header from a response.

    If the header is not present, return None.
    """
    etag =  r.headers.get('etag', None)
    if etag is None:
        return None

    if etag.startswith('"') and etag.endswith('"'):
        etag = etag[1:-1]
    elif etag.startswith("'") and etag.endswith("'"):
        etag = etag[1:-1]
    
    return etag


def get_last_modified(r: requests.Response) -> Optional[str]:
    """Get the `last-modified` header from a response.

    If the header is not present, return None.
    """
    return r.headers.get('last-modified', None)


@runtimeTypeCheck()
def float_last_modified(r_or_string: Union[Optional[requests.Response], Optional[str]]) -> Optional[float]:
    """Get the timestamp from a '%a, %d %b %Y %H:%M:%S %Z' string or a response. """
    if r_or_string is None:
        return None
    
    if isinstance(r_or_string, requests.Response):
        last_modified = get_last_modified(r_or_string)
    else:
        last_modified = r_or_string
    
    if last_modified is None:
        return None

    return time.mktime(time.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z'))


def get_content_disposition(r: requests.Response) -> Optional[str]:
    """Get the `content-disposition` header from a response.

    If the header is not present, return None.
    """
    return r.headers.get('content-disposition', None)


@runtimeTypeCheck()
def get_suggested_filename(r_or_string: Union[Optional[requests.Response], Optional[str]]) -> Optional[str]:
    """Get the suggested filename from a `content-disposition` string or a response. """
    if r_or_string is None:
        return None
    
    if isinstance(r_or_string, requests.Response):
        content_disposition = get_content_disposition(r_or_string)
        suggested_filename: Optional[str] = pyrfc6266.parse_filename(content_disposition) if content_disposition else None
    elif isinstance(r_or_string, str):
        suggested_filename: Optional[str] = pyrfc6266.parse_filename(r_or_string)
    else:
        raise TypeError(f"Expected requests.Response or str, got {type(r_or_string)}")

    return suggested_filename