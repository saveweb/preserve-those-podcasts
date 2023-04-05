from preserve_podcasts.utils.type_check import runtimeTypeCheck


NTFS_CHARS = r'<>:"/\|?*'
UNPRINTABLE_CHARS = ''.join(chr(i) for i in range(32))


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