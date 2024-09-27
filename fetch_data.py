import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional, Set

import aiohttp
from tqdm.asyncio import tqdm

from async_ckan import AsyncRemoteCKAN

BASE_URL = "https://bdap-opendata.rgs.mef.gov.it/SpodCkanApi/api/3/action/"


class CKAN:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        output_path: Path,
        mime_types_to_download: Optional[Set[str]] = None,
    ):
        self._ckan = AsyncRemoteCKAN(BASE_URL, get_only=True, session=session)
        # Don't be a smartass, thanks.
        self._ckan.base_url = ""

        self._cache_path = output_path / ".cache"
        self._cache_path.mkdir(parents=True, exist_ok=True)

        self._output_path = output_path
        self._output_path.mkdir(parents=True, exist_ok=True)

        self.supported_mime_types = mime_types_to_download or [
            "application/json",
            "application/x-javascript",
            "text/javascript",
            "text/x-javascript",
            "text/x-json",
            "text/csv",
        ]
        self._session = session

    async def _download(
        self,
        resource: Dict[str, Any],
        download_path: Path,
    ):
        url = resource.get("url", "")
        if not url:
            # Can't download shit without an URL 🫠
            return

        if url.startswith("http://"):
            # 🤦‍♀️
            url = url.replace("http://", "https://")

        name = resource.get("name", "")
        if not name:
            # An ugly name is better than the proper name I guess
            name = url.split("/")[-1]

        # There are invalid chars in file names
        name = name.replace("/", "-")

        download_path = download_path / name
        if download_path.exists():
            return

        max_retries = 10
        # Try some times, servers are wonky
        last_exc = None
        for _ in range(max_retries):
            try:
                async with self._session.get(url) as res:
                    res.raise_for_status()
                    total = int(res.headers.get("Content-Length", 0))
                    with download_path.open("wb") as fd, tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        desc=name,
                    ) as progress:
                        async for chunk in res.content.iter_any():
                            fd.write(chunk)
                            progress.update(len(chunk))
                # SUCCESS!
                break
            except Exception as exc:
                last_exc = exc
                continue
        else:
            # Servers are being stupid I guess 🤷
            with (self._output_path / "errors.log").open("a") as f:
                f.write(f"Couldn't download {url}: {last_exc}")

    async def group_list(self):
        return await self._ckan.action.group_list()

    async def dump_package(self, package_id: str, download_path: Path):
        cache = self._cache_path / f"{package_id}.json"
        if not cache.exists():
            package = await self._ckan.action.package_show(id=package_id)
            cache.write_text(json.dumps(package))
        else:
            package = json.loads(cache.read_text())

        if author := package.get("author", ""):
            download_path = download_path / author

        download_path.mkdir(parents=True, exist_ok=True)

        tasks = []
        for resource in package.get("resources", []):
            mimetype = resource.get("mimetype", "").lower()
            if mimetype not in self.supported_mime_types:
                continue
            url = resource.get("url", "")
            if not url:
                continue
            task = self._download(resource, download_path=download_path)
            tasks.append(task)

        await asyncio.gather(*tasks)

        (download_path / "metadata.json").write_text(json.dumps(package))

    async def dump_group(self, group_id: str):
        cache = self._cache_path / f"{group_id}.json"
        if not cache.exists():
            group = await self._ckan.action.group_show(id=group_id)
            cache.write_text(json.dumps(group))
        else:
            group = json.loads(cache.read_text())

        name = group.get("name", "")

        download_path = self._output_path / name
        download_path.mkdir(parents=True, exist_ok=True)

        tasks = []
        for package_id in group.get("packages", []):
            task = self.dump_package(package_id, download_path)
            tasks.append(task)

        await asyncio.gather(*tasks)
        (download_path / "metadata.json").write_text(json.dumps(group))

    async def dump_all(self):
        dataset_ids = await self._ckan.action.package_list()

        tasks = []
        for id in dataset_ids:
            task = self.dump_package(id, self._output_path)
            tasks.append(task)

        await asyncio.gather(*tasks)


groups = [
    # "163_anagrafe-enti-della-pubblica-amministrazione",
    # "151_bilanci-degli-enti-della-pubblica-amministrazione",
    "63_bilancio-finanziario-dello-stato",
    # "152_debito-degli-enti-della-pubblica-amministrazione",
    # "74_gestione-delle-spese-dello-stato",
    # "81_gestione-di-cassa-degli-enti-della-pubblica-amministrazione",
    # "146_litalia-e-lunione-europea",
    # "172_opere-pubbliche",
    # "82_pubblico-impiego",
    # "181_rendiconto",
    # "432_rendiconto",
    # "77_sanit",
    # "195_tesoreria",
]


async def main():
    dataset_path = Path(__file__).parent / "dataset"
    connector = aiohttp.TCPConnector(limit_per_host=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        ckan = CKAN(session, dataset_path)
        tasks = []
        for g in groups:
            t = ckan.dump_group(g)
            tasks.append(t)

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
