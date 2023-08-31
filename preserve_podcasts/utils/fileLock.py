import os
from pathlib import Path
import importlib.util
import time
from typing import Any

class AlreadyRunningError(Exception):
    def __init__(self, message: str=""):
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message

class FileLock_Basic:
    def __init__(self, lock_dir: Path, lock_filename):
        self.lock_file = lock_dir / lock_filename

    def __enter__(self):
        if os.path.exists(self.lock_file):
            with open(self.lock_file, 'r', encoding='utf-8') as f:
                data = f.read()
            raise AlreadyRunningError(f'Another instance is already running. ({data}) ({self.lock_file})')
        else:
            with open(self.lock_file, 'w', encoding='utf-8') as f:
                f.write(f'{os.getpid()}\t{int(time.time())}')
            # print("Acquired lock, continuing.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.lock_file)
        # print("Released lock.")

    # decorator
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper


class FileLock_Fcntl:
    fcntl = None
    try:
        import fcntl
    except ModuleNotFoundError:
        pass

    def __init__(self, lock_dir, lock_filename):
        if self.fcntl is None:
            raise(ModuleNotFoundError("No module named 'fcntl'", name='fcntl'))

        self.lock_file = Path(lock_dir) / lock_filename
        self.lock_file_fd = None

    def __enter__(self):
        assert self.fcntl is not None
        self.lock_file_fd = open(self.lock_file, 'wb', buffering=0)
        try:
            self.fcntl.lockf(self.lock_file_fd, self.fcntl.LOCK_EX | self.fcntl.LOCK_NB)
            # print("Acquired lock, continuing.")
            self.lock_file_fd.write(f'{os.getpid()}\t{int(time.time())}'.encode('utf-8'))
        except IOError:
            self.lock_file_fd = open(self.lock_file, 'r', encoding='utf-8')
            raise AlreadyRunningError(
                f"Another instance is already running. ({self.lock_file_fd.read()}) ({self.lock_file})")

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.fcntl is not None
        if self.lock_file_fd is None:
            raise IOError("Lock file not opened.")
        self.fcntl.lockf(self.lock_file_fd, self.fcntl.LOCK_UN)
        self.lock_file_fd.close() # lock_file_fd.close() 之后，其他进程有机会在本进程删掉锁文件之前拿到新锁
        try:
            os.remove(self.lock_file) # 删除文件不影响其他进程已持有的 inode 新锁
        except FileNotFoundError:
            # 如果抢到新锁的是本进程，删除文件的是其他进程，那么本进程再删除时自然会 FileNotFoundError，忽略就好
            pass
        # print("Released lock.")

    # decorator
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper

class FileLock:
    def __new__(cls, lock_dir: Path, lock_filename: str):
        """
        lock_dir: 要在哪个目录下创建锁文件
        lock_filename: 锁文件的文件名
        """
        if isinstance(lock_dir, str):
            lock_dir = Path(lock_dir)

        lock_dir.mkdir(parents=True, exist_ok=True)

        fcntl_avaivable = importlib.util.find_spec('fcntl')
        if fcntl_avaivable is not None:
            return FileLock_Fcntl(lock_dir, lock_filename)
        else:
            return FileLock_Basic(lock_dir, lock_filename)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        def decorator(func):
            def wrapper(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    