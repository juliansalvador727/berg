"""Fetch TrainStats' daily JSON archive into data/datasets/it/raw/trainstats.

TrainStats publishes one file per service day in two places (see docs/data-notes-it.md):

  Dropbox   dati_YYYY_MM_DD.zip, one JSON each, 2024-06-08 onward. A public shared folder:
            its dl=1 link streams the whole folder as one ZIP, which is the only anonymous
            way in (single files need a Dropbox API app). ~1.6 GB.
  Mega      the legacy archive through 2024-06-07, plain JSON. Only the two folders written
            by TrainStats' current database (2023-02-01 onward, files named dati_YYYY_MM_DD)
            are fetched; older folders hold the previous database's format.

Every day ends up under days/ as the file the source served (Dropbox inner ZIP) or its gzip
(Mega JSON, which is stored uncompressed at the source). ledger.csv records where each came
from with its SHA-256; raw files never go to R2.
"""

import base64
import csv
import gzip
import hashlib
import io
import json
import os
import struct
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from berg_pipeline.europe.config import DatasetConfig

DROPBOX_URL = (
    "https://www.dropbox.com/scl/fo/uv5rz6y6gqpkpciyymg0b/ALu4uc7oD0v_iGdFsSYYJZE"
    "?rlkey=smo0xhqe4ha7bxjnwfsooagg3&dl=1"
)
MEGA_FOLDER = "aQRUAAiY"
MEGA_KEY = "rGGhCFMaera3oVxDEG3zBQ"
MEGA_URL = f"https://mega.nz/folder/{MEGA_FOLDER}#{MEGA_KEY}"
MEGA_API = "https://g.api.mega.co.nz/cs"
# Mega folder handles of the current-database exports: "01_02_2023 a 31_12_2023" and the
# root "Dati TrainStats v2" (2024-01-01..2024-06-07).
MEGA_CURRENT_FOLDERS = ("TNInBRJb", "nQR3FTaY")
USER_AGENT = "berg/europe-archive (historical train map; sequential downloads)"
LEDGER_COLS = (
    "service_day archive_source archive_filename remote_size local_size sha256 retrieved_at status"
).split()


def raw_root(cfg: DatasetConfig) -> Path:
    return cfg.raw_dir / "trainstats"


def day_file(cfg: DatasetConfig, day: date) -> Path | None:
    """The local raw file of one service day, whichever source it came from."""
    days = raw_root(cfg) / "days"
    for name in (f"dati_{day:%Y_%m_%d}.zip", f"dati_{day:%Y_%m_%d}.json.gz"):
        if (days / name).exists():
            return days / name
    return None


def read_day(path: Path) -> dict:
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            (member,) = z.namelist()
            return json.loads(z.read(member))
    with gzip.open(path, "rb") as fh:
        return json.loads(fh.read())


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_ledger(cfg: DatasetConfig) -> dict[str, dict]:
    path = raw_root(cfg) / "ledger.csv"
    if not path.exists():
        return {}
    with open(path, newline="") as fh:
        return {r["service_day"]: r for r in csv.DictReader(fh)}


def _write_ledger(cfg: DatasetConfig, ledger: dict[str, dict]) -> None:
    path = raw_root(cfg) / "ledger.csv"
    tmp = path.with_name(".ledger.csv.tmp")
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, LEDGER_COLS)
        w.writeheader()
        for k in sorted(ledger):
            w.writerow(ledger[k])
    os.replace(tmp, path)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_name(name: str) -> date:
    stem = Path(name).name.split(".")[0]  # dati_YYYY_MM_DD
    y, m, d = stem.split("_")[1:4]
    return date(int(y), int(m), int(d))


# --- Dropbox ------------------------------------------------------------------------------


def fetch_dropbox(cfg: DatasetConfig, refresh: bool = False) -> int:
    """The whole shared folder as one ZIP → days/. Returns the number of new days.

    The folder ZIP is kept and reused; --refresh downloads it again to pick up new days.
    A day already in days/ with the same bytes is left alone.
    """
    import httpx

    root = raw_root(cfg)
    bundle = root / "dropbox_trainstats.zip"
    if refresh or not bundle.exists():
        tmp = bundle.with_name(bundle.name + ".part")
        root.mkdir(parents=True, exist_ok=True)
        for attempt in range(1, 6):
            try:
                with httpx.stream(
                    "GET",
                    DROPBOX_URL,
                    headers={"User-Agent": USER_AGENT},
                    timeout=600,
                    follow_redirects=True,
                ) as r:
                    r.raise_for_status()
                    expected = int(r.headers.get("content-length", 0))
                    with open(tmp, "wb") as fh:
                        for chunk in r.iter_bytes(1 << 20):
                            fh.write(chunk)
                if expected and tmp.stat().st_size != expected:
                    raise OSError(f"truncated: {tmp.stat().st_size} of {expected} bytes")
                os.replace(tmp, bundle)
                break
            except Exception:
                if attempt == 5:
                    raise
                time.sleep(30 * attempt)

    ledger = load_ledger(cfg)
    days = root / "days"
    days.mkdir(parents=True, exist_ok=True)
    added = 0
    with zipfile.ZipFile(bundle) as z:
        for info in z.infolist():
            name = Path(info.filename).name
            if not (name.startswith("dati_") and name.endswith(".zip")):
                continue
            day = _parse_name(name)
            out = days / name
            data = z.read(info)
            digest = hashlib.sha256(data).hexdigest()
            row = ledger.get(day.isoformat())
            if row and row["sha256"] == digest and (out.exists() or row["status"] != "ok"):
                continue
            # A file TrainStats itself uploaded truncated is recorded, never stored: the day
            # is a source gap.
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as inner:
                    status = "ok" if inner.testzip() is None else "corrupt_at_source"
            except zipfile.BadZipFile:
                status = "truncated_at_source"
            if status == "ok":
                tmp = out.with_name(f".{name}.tmp")
                tmp.write_bytes(data)
                os.replace(tmp, out)
            else:
                print(f"{name}: {status}", flush=True)
            ledger[day.isoformat()] = {
                "service_day": day.isoformat(),
                "archive_source": "dropbox",
                "archive_filename": info.filename,
                "remote_size": info.file_size,
                "local_size": len(data),
                "sha256": digest,
                "retrieved_at": _now(),
                "status": status,
            }
            added += status == "ok"
    _write_ledger(cfg, ledger)
    return added


# --- Mega ---------------------------------------------------------------------------------


def _b64d(s: str) -> bytes:
    s = s.replace("-", "+").replace("_", "/").replace(",", "")
    return base64.b64decode(s + "=" * (-len(s) % 4))


def mega_listing(client) -> list[dict]:
    """Every file of the public folder: name, size, handle, parent, AES key and CTR nonce."""
    from Crypto.Cipher import AES

    folder_key = _b64d(MEGA_KEY)
    r = client.post(
        f"{MEGA_API}?id=0&n={MEGA_FOLDER}", json=[{"a": "f", "c": 1, "r": 1, "ca": 1}]
    ).json()
    out = []
    for n in r[0]["f"]:
        if n["t"] != 0:
            continue
        raw = AES.new(folder_key, AES.MODE_ECB).decrypt(_b64d(n["k"].split(":")[-1]))
        k = struct.unpack(">8I", raw)
        key = struct.pack(">4I", k[0] ^ k[4], k[1] ^ k[5], k[2] ^ k[6], k[3] ^ k[7])
        attr = AES.new(key, AES.MODE_CBC, b"\0" * 16).decrypt(_b64d(n["a"])).rstrip(b"\0")
        name = json.loads(attr[4:].decode())["n"]
        out.append(
            {
                "name": name,
                "size": n["s"],
                "handle": n["h"],
                "parent": n["p"],
                "key": key,
                "nonce": struct.pack(">2I", k[4], k[5]),
            }
        )
    return out


class MegaQuotaExceeded(RuntimeError):
    pass


def _mega_download(client, node: dict) -> bytes:
    from Crypto.Cipher import AES
    from Crypto.Util import Counter

    g = client.post(
        f"{MEGA_API}?id=1&n={MEGA_FOLDER}", json=[{"a": "g", "g": 1, "n": node["handle"]}]
    ).json()
    if isinstance(g, int) or isinstance(g[0], int):
        code = g if isinstance(g, int) else g[0]
        if code in (-17, -4):  # EOVERQUOTA / ERATELIMIT
            raise MegaQuotaExceeded(str(code))
        raise RuntimeError(f"mega API error {code} for {node['name']}")
    r = client.get(g[0]["g"], timeout=900)
    if r.status_code == 509:
        raise MegaQuotaExceeded("509")
    r.raise_for_status()
    ctr = Counter.new(128, initial_value=int.from_bytes(node["nonce"] + b"\0" * 8, "big"))
    return AES.new(node["key"], AES.MODE_CTR, counter=ctr).decrypt(r.content)


def fetch_mega(cfg: DatasetConfig, first: date, last: date, quota_wait_s: int = 1800) -> int:
    """Legacy current-database days in [first, last] → days/*.json.gz, oldest first.

    Anonymous Mega downloads have a transfer quota of a few GB per several hours; when it is
    hit this sleeps and resumes. A file shorter than Mega's listed size, or one that is not
    valid JSON, is a failed download and is retried, never stored.
    """
    import httpx

    days = raw_root(cfg) / "days"
    days.mkdir(parents=True, exist_ok=True)
    ledger = load_ledger(cfg)
    added = 0
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120) as client:
        nodes = [
            n
            for n in mega_listing(client)
            if n["parent"] in MEGA_CURRENT_FOLDERS
            and n["name"].startswith("dati_")
            and n["name"].endswith(".json")
        ]
        nodes = sorted(
            (n for n in nodes if first <= _parse_name(n["name"]) <= last),
            key=lambda n: n["name"],
        )
        for node in nodes:
            day = _parse_name(node["name"])
            if day_file(cfg, day) is not None:
                continue
            for attempt in range(1, 50):
                try:
                    data = _mega_download(client, node)
                    if len(data) != node["size"]:
                        raise OSError(f"truncated: {len(data)} of {node['size']} bytes")
                    json.loads(data)
                    break
                except MegaQuotaExceeded as e:
                    print(f"mega quota ({e}) at {day}; sleeping {quota_wait_s}s", flush=True)
                    time.sleep(quota_wait_s)
                except Exception as e:
                    if attempt > 5:
                        raise
                    print(f"{day}: {e!r}, retrying", flush=True)
                    time.sleep(20 * attempt)
            out = days / f"dati_{day:%Y_%m_%d}.json.gz"
            tmp = out.with_name(f".{out.name}.tmp")
            with gzip.GzipFile(tmp, "wb", mtime=0) as fh:
                fh.write(data)
            os.replace(tmp, out)
            ledger[day.isoformat()] = {
                "service_day": day.isoformat(),
                "archive_source": "mega",
                "archive_filename": node["name"],
                "remote_size": node["size"],
                "local_size": out.stat().st_size,
                "sha256": hashlib.sha256(data).hexdigest(),
                "retrieved_at": _now(),
                "status": "ok",
            }
            _write_ledger(cfg, ledger)
            added += 1
            print(f"mega {day}: {len(data) / 1e6:.1f} MB", flush=True)
    return added
