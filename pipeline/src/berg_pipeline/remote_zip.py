"""Read one member out of a remote ZIP without downloading the whole archive.

The Ist-Daten archive serves range requests and a ZIP keeps its central directory at the end,
so a handful of ranges gets a member list, and one more gets a single service day (~47 MB) out
of a 1.4 GB month. See docs/data-notes.md.
"""

import io
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager

import httpx

# Big enough that sequential reads inside a member don't turn into a request per call, small
# enough that reading the central directory doesn't pull megabytes.
CHUNK_SIZE = 1 << 20


class HttpRangeFile(io.RawIOBase):
    """A seekable read-only file over an HTTP resource, backed by range requests."""

    def __init__(self, client: httpx.Client, url: str):
        self._client = client
        self._url = url
        self._pos = 0

        head = client.head(url)
        head.raise_for_status()
        if head.headers.get("accept-ranges") != "bytes":
            raise RuntimeError(f"{url} does not advertise range support")
        self._size = int(head.headers["content-length"])

    @property
    def size(self) -> int:
        return self._size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = max(0, base + offset)
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self._size - self._pos
        size = min(size, self._size - self._pos)
        if size <= 0:
            return b""
        end = self._pos + size - 1
        r = self._client.get(self._url, headers={"Range": f"bytes={self._pos}-{end}"})
        r.raise_for_status()
        data = r.content
        self._pos += len(data)
        return data

    def readinto(self, b) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)


@contextmanager
def open_remote_zip(url: str, timeout: float = 60.0, retries: int = 3) -> Iterator[zipfile.ZipFile]:
    """Yield a ZipFile over a remote archive. Members read lazily, by range.

    Connection-level retries matter here: a ZIP is read over many small ranges, so one dropped
    connection out of hundreds surfaces as BadZipFile ("not a zip file") rather than a network
    error — which sends you hunting for a corrupt archive that is fine. Seen under ~12-way
    concurrency against this host.
    """
    transport = httpx.HTTPTransport(retries=retries)
    with httpx.Client(follow_redirects=True, timeout=timeout, transport=transport) as client:
        raw = HttpRangeFile(client, url)
        # BufferedReader keeps zipfile's many small seeks/reads from becoming many requests.
        with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=CHUNK_SIZE)) as zf:
            yield zf
