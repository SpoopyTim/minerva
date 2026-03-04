from pathlib import Path

import httpx

from minerva.downloaders import Downloader, ProgressCallback


class HTTPX(Downloader):
    async def __call__(
        self, url: str, dest: Path, size: int, connections: int, pre_allocation: str, on_progress: ProgressCallback
    ) -> None:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15, read=300, write=60, pool=10),
        ) as client:
            async with client.stream("GET", url) as resp:
                total_size = size

                if resp and resp.is_success:
                    content_length = resp.headers.get("Content-Length")
                    if content_length is not None:
                        try:
                            total_size = int(content_length)
                        except ValueError:
                            pass
                else:
                    if on_progress:
                        on_progress(0, total_size)
                    resp.raise_for_status()

                downloaded = 0
                with open(dest, "wb") as f:
                    async for chunk in resp.aiter_bytes(1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size is not None:
                            on_progress(downloaded, total_size)

                if on_progress:
                    on_progress(downloaded, total_size or downloaded)


__all__ = ["HTTPX"]
