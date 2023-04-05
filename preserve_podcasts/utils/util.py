from preserve_podcasts.utils.type_check import runtimeTypeCheck


NTFS_CHARS = r'<>:"/\|?*'

@runtimeTypeCheck()
def replace_ntfs_chars(s: str) -> str:
    """Replace NTFS reserved characters with underscores."""
    return ''.join(c if c not in NTFS_CHARS else '_' for c in s)