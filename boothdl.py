#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Booth.pm Purchased Items Downloader (fast-start, minimized tweaks)

- Fetches orders from https://accounts.booth.pm/orders
- Extracts each item's storefront page id + canonical title
- Downloads:
  * all files from `/downloadables/{id}`
  * the first storefront image (OpenGraph/Twitter image)
- Names:  {pageid} - {item title}/{pageid} - {downloaded file name}
- Manifest cache (_manifest.json at script root) to skip repeats
- HTTP/2 (httpx) for HTML; requests for streamed file downloads
- Targeted cookie load: cookies.txt first, then a single browser/domain
- Optional: skip auth preflight to start indexing faster
- Pacing/batching + backoff (403/429), atomic renames (Win “file in use”)

Python 3.9+
"""

from __future__ import annotations

import html
import json
import logging
import mimetypes
import os
import pathlib
import random
import re
import tempfile
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

__version__ = "1.2.0-min"

# ---------- Optional deps ----------
try:
    import httpx  # HTTP/2 for HTML
except Exception:
    httpx = None

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except Exception:
    BS_PARSER = "html.parser"

try:
    import browser_cookie3  # auto cookie pickup
except Exception:
    browser_cookie3 = None

# ---------- Constants ----------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
ROOT = pathlib.Path("boothdl"); ROOT.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = SCRIPT_DIR / "_manifest.json"
MAN_LOCK = threading.Lock()
MAN_DATA: Dict[str, Dict[str, Dict[str, str]]] = {"downloads": {}, "images": {}}

S = requests.Session(); S.headers.update(HEADERS)
H2 = None  # httpx.Client (HTTP/2) for HTML
INDEX = "https://accounts.booth.pm/orders"
FORBIDDEN_SLEEP = 300.0

R_STOREFRONT_ITEM = re.compile(r"https?://([a-z0-9\-]+)\.booth\.pm/(?:[a-z]{2}/)?items/(\d+)", re.I)
R_ANY_ITEM       = re.compile(r"(?:https?://(?:www\.)?booth\.pm/(?:[a-z]{2}/)?items/(\d+))|(?:/items/(\d+))", re.I)
R_DL             = re.compile(r"(?:https?://booth\.pm)?/downloadables/(\d+)", re.I)
R_ID_PREFIX      = re.compile(r"^(\d+)\s*-\s*")
SAFE             = re.compile(r"[^-_.() \w]+", re.UNICODE)

# ---------- Logging / HTTP ----------
def log_setup(verbosity: int) -> None:
    level = logging.DEBUG if verbosity >= 2 else logging.INFO if verbosity == 1 else logging.WARNING
    logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

def cfg_http(pool: int) -> None:
    ad = HTTPAdapter(
        pool_connections=pool, pool_maxsize=pool,
        max_retries=Retry(total=3, backoff_factor=0.4, status_forcelist=[429,500,502,503,504]),
    )
    S.mount("https://", ad); S.mount("http://", ad)

class RateLimiter:
    def __init__(self, min_interval: float = 0.0, jitter: float = 0.0):
        self.min_interval = max(0.0, float(min_interval))
        self.jitter = max(0.0, float(jitter))
        self._next = 0.0
        self._lock = threading.Lock()
    def sleep(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self.min_interval > 0:
                if now < self._next:
                    time.sleep(self._next - now)
                    now = time.monotonic()
                delay = self.min_interval + (random.uniform(-self.jitter, self.jitter) if self.jitter>0 else 0.0)
                self._next = now + max(0.0, delay)
            else:
                self._next = now

RL = RateLimiter()

def throttled_get(url: str, **kwargs):
    RL.sleep()
    timeout = kwargs.pop("timeout", 30); allow_redirects = kwargs.pop("allow_redirects", True)
    for attempt in range(7):
        r = S.get(url, timeout=timeout, allow_redirects=allow_redirects, **kwargs)
        if r.status_code in (429, 503):
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.replace(".","",1).isdigit() else (2.0*(attempt+1))
            logging.warning(f"HTTP {r.status_code} on GET {url} — sleeping {wait:.1f}s")
            time.sleep(wait); continue
        if r.status_code == 403:
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.replace(".","",1).isdigit() else FORBIDDEN_SLEEP
            logging.error(f"HTTP 403 on GET {url} — pausing for {wait:.1f}s to cool down")
            time.sleep(wait); continue
        return r
    return r

def fetch_html(url: str):
    RL.sleep()
    if H2 is not None:
        try:
            r = H2.get(url, follow_redirects=True)
            if r.status_code in (429, 503):
                ra = r.headers.get("retry-after")
                wait = float(ra) if ra and ra.replace(".","",1).isdigit() else 2.0
                time.sleep(wait); r = H2.get(url, follow_redirects=True)
            if r.status_code == 403:
                ra = r.headers.get("retry-after")
                wait = float(ra) if ra and ra.replace(".","",1).isdigit() else FORBIDDEN_SLEEP
                time.sleep(wait); r = H2.get(url, follow_redirects=True)
            return r
        except Exception:
            pass
    return throttled_get(url)

def init_http2_client() -> None:
    global H2
    if httpx is None:
        H2 = None; return
    try:
        cookies = httpx.Cookies()
        for c in S.cookies:
            try: cookies.set(c.name, c.value, domain=c.domain, path=c.path)
            except Exception: cookies.set(c.name, c.value)
        H2 = httpx.Client(http2=True, headers=HEADERS, cookies=cookies, timeout=30.0)
    except Exception:
        H2 = None

# ---------- Manifest ----------
def _manifest_load() -> None:
    global MAN_DATA
    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                MAN_DATA = json.load(f)
                MAN_DATA.setdefault("downloads", {}); MAN_DATA.setdefault("images", {})
        except Exception as e:
            logging.warning(f"manifest load failed: {e}")

def _manifest_save() -> None:
    try:
        tmp = MANIFEST_PATH.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(MAN_DATA, f, ensure_ascii=False, indent=2)
        os.replace(tmp, MANIFEST_PATH)
    except Exception as e:
        logging.warning(f"manifest save failed: {e}")

def manifest_get_download(url: str) -> Optional[pathlib.Path]:
    with MAN_LOCK: entry = MAN_DATA.get("downloads", {}).get(url)
    if not entry: return None
    p = ROOT / entry.get("path",""); return p if p.exists() else None

def manifest_set_download(url: str, out: pathlib.Path, headers: Optional[dict] = None) -> None:
    rel = str(out.relative_to(ROOT)); meta = {}
    if headers:
        for k in ("ETag","Last-Modified","Content-Length","Content-Type"):
            v = headers.get(k) or headers.get(k.lower())
            if v: meta[k] = str(v)
    with MAN_LOCK: MAN_DATA["downloads"][url] = {"path": rel, **meta}
    _manifest_save()

def manifest_get_image(url: str) -> Optional[pathlib.Path]:
    with MAN_LOCK: entry = MAN_DATA.get("images", {}).get(url)
    if not entry: return None
    p = ROOT / entry.get("path",""); return p if p.exists() else None

def manifest_set_image(url: str, out: pathlib.Path, headers: Optional[dict] = None) -> None:
    rel = str(out.relative_to(ROOT)); meta = {}
    if headers:
        for k in ("ETag","Last-Modified","Content-Length","Content-Type"):
            v = headers.get(k) or headers.get(k.lower())
            if v: meta[k] = str(v)
    with MAN_LOCK: MAN_DATA["images"][url] = {"path": rel, **meta}
    _manifest_save()

# ---------- Cookies (targeted + fast) ----------
def _has_booth_session(jar: requests.cookies.RequestsCookieJar) -> bool:
    if not jar: 
        return False
    domains = {c.domain for c in jar}
    return any(d.endswith("booth.pm") for d in domains) or ("accounts.booth.pm" in domains)

def load_cookies_file(p: str) -> requests.cookies.RequestsCookieJar:
    jar = requests.cookies.RequestsCookieJar()
    try:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                if not line or line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 7:
                    continue
                d, _flag, path, secure, _exp, name, val = parts
                jar.set_cookie(
                    requests.cookies.create_cookie(
                        name=name, value=val, domain=d, path=path, secure=secure.upper()=="TRUE"
                    )
                )
    except Exception as e:
        logging.warning(f"cookies read fail {p}: {e}")
    return jar

def ensure_cookies_targeted(cookie_mode: str, browser: str | None, profile: str, cookies_file: str) -> None:
    """cookies.txt first, then a single browser/domain; stop when a booth session is found"""
    jar = requests.cookies.RequestsCookieJar(); used_sources: list[str] = []
    def add(j):
        nonlocal jar
        if not j: return
        for c in j: jar.set(c.name, c.value, domain=c.domain, path=c.path)

    if cookie_mode in ("auto", "file"):
        for p in ([cookies_file] if cookies_file else []) + ["cookies.txt"]:
            if p and os.path.exists(p):
                j = load_cookies_file(p); add(j)
                if j: used_sources.append(f"file:{p}")
                if _has_booth_session(jar): break

    if not _has_booth_session(jar) and cookie_mode in ("auto","browser") and browser_cookie3:
        funcs=[]
        if browser:
            fn=getattr(browser_cookie3, browser, None)
            if fn: funcs=[fn]
        if not funcs:
            for name in ("chrome","edge","brave","firefox","opera"):
                fn=getattr(browser_cookie3, name, None)
                if fn: funcs.append(fn)
        domains = [".booth.pm", "accounts.booth.pm", ".pixiv.net"]
        found=False
        for fn in funcs:
            try_profiles = [profile] if profile else [None]
            for prof in try_profiles:
                for d in domains:
                    try:
                        try: cj = fn(domain_name=d, profile=prof)
                        except TypeError:
                            try: cj = fn(domain_name=d)
                            except TypeError: cj = fn()
                        add(cj); used_sources.append(f"{fn.__name__}:{d}" + (f":{prof}" if prof else ""))
                        if _has_booth_session(jar): found=True; break
                    except Exception: continue
                if found: break
            if found: break

    try: S.cookies.update(jar)
    except Exception: pass

    if _has_booth_session(jar):
        logging.info("Loaded cookies for: " + ", ".join(sorted({c.domain for c in jar})))
        if used_sources: logging.debug("Cookie sources: " + ", ".join(used_sources))
    else:
        logging.warning("No cookies loaded; login via browser and rerun, or export cookies.txt (Get cookies.txt extension).")

def auth_ok() -> bool:
    try:
        r = throttled_get(INDEX)
        if any(s in r.url for s in ("accounts.pixiv.net", "login", "signin")) or any(
            w in r.text.lower() for w in ("ログイン", "sign in", "log in")
        ):
            logging.warning("Auth invalid."); return False
        logging.info("Auth check passed."); return True
    except Exception as e:
        logging.warning(f"Auth check failed: {e}"); return False

# ---------- HTML helpers ----------
def soupify(html_text: str) -> BeautifulSoup:
    return BeautifulSoup(html_text, BS_PARSER)

def orders_on_index(html_text: str) -> List[str]:
    soup = soupify(html_text); out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"): href = "https://accounts.booth.pm" + href
        if href.startswith("https://accounts.booth.pm/orders/"):
            u = href.split("?")[0].split("#")[0]
            if u not in seen: seen.add(u); out.append(u)
    return out

def last_page_from_index(html_text: str) -> int:
    soup = soupify(html_text); last = 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "orders?page=" in href:
            try: last = max(last, int(href.split("page=",1)[1].split("&")[0].split("#")[0]))
            except Exception: pass
    return last

def _closest_block(node):
    for _ in range(8):
        if node is None: return None
        if getattr(node, "name", "") in ("article","section","li","div"):
            cls = " ".join(node.get("class", []))
            if any(k in cls for k in ("order","item","product","purchase","list","entry","content")):
                return node
        node = node.parent
    return None

def _find_item_in_block(block):
    for a in block.find_all("a", href=True):
        m = R_STOREFRONT_ITEM.search(a["href"])
        if m:
            store, pid = m.group(1), m.group(2)
            title = a.get_text(" ", strip=True) or ""
            if not title:
                img = a.find("img") or block.find("img")
                if img and img.get("alt"): title = img["alt"]
            return pid, title, store
    for a in block.find_all("a", href=True):
        m = R_ANY_ITEM.search(a["href"])
        if m: return (m.group(1) or m.group(2)), (a.get_text(" ", strip=True) or ""), ""
    return "", "", ""

def _find_prev_item(anchor):
    prev = anchor
    for _ in range(200):
        prev = prev.find_previous("a")
        if not prev: break
        m = R_STOREFRONT_ITEM.search(prev.get("href",""))
        if m: return m.group(2), (prev.get_text(" ", strip=True) or ""), m.group(1)
        m2 = R_ANY_ITEM.search(prev.get("href",""))
        if m2: return (m2.group(1) or m2.group(2)), (prev.get_text(" ", strip=True) or ""), ""
    return "", "", ""

# ---------- Filenames ----------
def slug(s: str) -> str:
    return re.sub(r"\s+", " ", SAFE.sub("", html.unescape((s or "").replace("\n", " ").strip())))

def _normalize_windows_name(name: str) -> str:
    name = requests.utils.unquote(name)
    name = name.replace("\\", "＼").replace("/", "／").replace("\r", " ").replace("\n", " ").replace("\0", " ")
    reserved = {"CON","PRN","AUX","NUL"} | {f"COM{i}" for i in range(1,10)} | {f"LPT{i}" for i in range(1,10)}
    base, ext = os.path.splitext(name.strip())
    if base.upper() in reserved: base = f"_{base}"
    base = base.rstrip(" ."); return (base or "file")+ext

def name_from_resp(r: requests.Response, fallback: str) -> str:
    cd = r.headers.get("content-disposition","")
    for pat in (r"filename\*=UTF-8''([^;]+)", r'filename="([^"]+)"', r"filename=([^;]+)"):
        m = re.search(pat, cd)
        if m:
            raw = m.group(1).strip().strip('"')
            try: return _normalize_windows_name(requests.utils.unquote(raw))
            except Exception: return _normalize_windows_name(raw)
    url_name = pathlib.Path(requests.utils.urlparse(r.url).path).name
    return _normalize_windows_name(url_name or fallback or "download")

def resolve_name_and_size(url: str, headers: dict | None = None) -> Tuple[str, Optional[int]]:
    hdr = dict(headers or {})
    try:
        RL.sleep()
        r = S.head(url, allow_redirects=True, timeout=30, headers=hdr)
        if not r.ok or r.status_code in (405,403,404): raise RuntimeError(f"HEAD not usable: {r.status_code}")
        name = name_from_resp(r, pathlib.Path(url).name)
        size = int(r.headers.get("content-length","0")) if r.headers.get("content-length","0").isdigit() else None
        return name, size
    except Exception:
        RL.sleep()
        r = S.get(url, allow_redirects=True, timeout=30, headers=hdr, stream=True); r.raise_for_status()
        name = name_from_resp(r, pathlib.Path(url).name)
        size = int(r.headers.get("content-length","0")) if r.headers.get("content-length","0").isdigit() else None
        try: r.close()
        except Exception: pass
        return name, size

# --- cleaned chunk-size chooser ---
def choose_chunk(size: Optional[int], *, is_image: bool = False) -> int:
    """Return a sensible stream chunk size in bytes."""
    if is_image:
        return 256 * 1024
    if not size:  # unknown
        return 1 * 1024 * 1024
    # table: (threshold_bytes, chunk_bytes), first match wins
    for thresh, chunk in (
        (300 * 1024 * 1024, 4 * 1024 * 1024),
        (100 * 1024 * 1024, 2 * 1024 * 1024),
        ( 20 * 1024 * 1024, 1 * 1024 * 1024),
    ):
        if size >= thresh:
            return chunk
    return 512 * 1024

def _rename_with_retry(src: pathlib.Path, dst: pathlib.Path, *, attempts: int = 10, initial_sleep: float = 0.2) -> None:
    wait = initial_sleep
    for i in range(attempts):
        try: os.replace(str(src), str(dst)); return
        except PermissionError:
            if i == attempts-1: raise
            time.sleep(wait); wait = min(wait*1.6, 3.0)

_FILE_LOCKS = defaultdict(threading.Lock)

# ---------- Item meta / cover ----------
META_CACHE: Dict[str, Tuple[str, Optional[str], str]] = {}
META_LOCK = threading.Lock()

def get_canonical_title(store: str, pageid: str, title_hint: str) -> str:
    with META_LOCK:
        if pageid in META_CACHE: return META_CACHE[pageid][0]
    base = f"https://{store}.booth.pm" if store else "https://booth.pm"
    item_url, title, img_url = f"{base}/items/{pageid}", (title_hint or f"Item {pageid}"), None
    try:
        r = fetch_html(item_url)
        if r.status_code != 404:
            r.raise_for_status(); soup = BeautifulSoup(r.text, BS_PARSER)
            title = (soup.find("meta", attrs={"property":"og:title"}) or {}).get("content") or title
            for sel in (("meta", {"property":"og:image"}), ("meta", {"name":"twitter:image"})):
                tag = soup.find(*sel)
                if tag and tag.get("content"): img_url = tag["content"].split("?")[0]; break
    except Exception:
        pass
    with META_LOCK: META_CACHE[pageid] = (title, img_url, base)
    return title

def save_primary_image(store: str, pageid: str, title_hint: str, target_folder: Optional[pathlib.Path] = None) -> None:
    title = get_canonical_title(store, pageid, title_hint)
    base = f"https://{store}.booth.pm" if store else "https://booth.pm"
    item_url = f"{base}/items/{pageid}"
    r = fetch_html(item_url)
    if r.status_code == 404: return
    r.raise_for_status(); soup = BeautifulSoup(r.text, BS_PARSER)
    folder = target_folder if target_folder is not None else (ROOT / f"{pageid} - {slug(title)}")
    folder.mkdir(parents=True, exist_ok=True)

    img_url = ""
    for sel in (("meta", {"property": "og:image"}), ("meta", {"name": "twitter:image"})):
        tag = soup.find(*sel)
        if tag and tag.get("content"): img_url = tag["content"].split("?")[0]; break
    if not img_url: return

    cached_img = manifest_get_image(img_url)
    if cached_img and cached_img.exists():
        logging.info(f"Cover exists (manifest): {cached_img}"); return

    try:
        hdr = {**HEADERS, "Referer": base + "/"}
        with throttled_get(img_url, headers=hdr, timeout=60, stream=True) as resp:
            resp.raise_for_status()
            ct = (resp.headers.get("content-type","") or "").split(";")[0].strip()
            ext = mimetypes.guess_extension(ct) or os.path.splitext(pathlib.Path(requests.utils.urlparse(resp.url).path).name)[1] or ".jpg"
            if not ext.startswith("."): ext = "."+ext
            out = folder / f"{pageid} - {slug(title)}{ext}"
            if out.exists(): manifest_set_image(img_url, out, resp.headers); return
            fd, tmp_path = tempfile.mkstemp(prefix=out.stem + ".", suffix=".part", dir=str(folder))
            os.close(fd); tmp = pathlib.Path(tmp_path)
            try:
                total = int(resp.headers.get("content-length","0")) or None
                chunk = choose_chunk(total, is_image=True)
                with open(tmp,"wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=out.name) as p:
                    for c in resp.iter_content(chunk_size=chunk):
                        if not c: continue
                        f.write(c); p.update(len(c)) if total else None
                try:
                    with open(tmp,"rb+") as f2: f2.flush(); os.fsync(f2.fileno())
                except Exception: pass
                if out.exists(): 
                    try: tmp.unlink(missing_ok=True)
                    except Exception: pass
                    manifest_set_image(img_url, out, resp.headers); return
                _rename_with_retry(tmp, out); manifest_set_image(img_url, out, resp.headers)
            finally:
                try:
                    if tmp.exists(): tmp.unlink()
                except Exception: pass
    except Exception as e:
        logging.debug(f"primary image fail: {e}")

# ---------- Download ----------
def stream_download(url: str, folder: pathlib.Path, pageid: str) -> None:
    cached = manifest_get_download(url)
    if cached:
        logging.info(f"Exists (manifest), skip: {cached.name}"); return

    name, size = resolve_name_and_size(url, headers=HEADERS)
    out = folder / f"{pageid} - {name}"
    if out.exists():
        manifest_set_download(url, out, {}); logging.info(f"Exists, skip: {out.name}"); return

    lock = _FILE_LOCKS[str(folder).lower()]
    with lock:
        folder.mkdir(parents=True, exist_ok=True)
        with throttled_get(url, timeout=180, allow_redirects=True, stream=True) as r:
            r.raise_for_status()
            name2 = name_from_resp(r, name)
            out2 = folder / f"{pageid} - {name2}"
            if out2.exists():
                manifest_set_download(url, out2, r.headers); logging.info(f"Exists, skip: {out2.name}"); return
            fd, tmp_path = tempfile.mkstemp(prefix=out2.stem + ".", suffix=".part", dir=str(folder))
            os.close(fd); tmp = pathlib.Path(tmp_path)
            try:
                total = int(r.headers.get("content-length","0")) or size or None
                chunk = choose_chunk(total, is_image=False)
                with open(tmp,"wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=out2.name) as p:
                    for c in r.iter_content(chunk_size=chunk):
                        if not c: continue
                        f.write(c); p.update(len(c)) if total else None
                try:
                    with open(tmp,"rb+") as f2: f2.flush(); os.fsync(f2.fileno())
                except Exception: pass
                if out2.exists():
                    try: tmp.unlink(missing_ok=True)
                    except Exception: pass
                    manifest_set_download(url, out2, r.headers); return
                _rename_with_retry(tmp, out2); manifest_set_download(url, out2, r.headers)
            finally:
                try:
                    if tmp.exists(): tmp.unlink()
                except Exception: pass

def batched(iterable: Iterable, n: int):
    batch = []; 
    for x in iterable:
        batch.append(x)
        if len(batch) >= n:
            yield batch; batch = []
    if batch: yield batch

# ---------- FS cover refresh ----------
def ids_from_fs(root: pathlib.Path) -> List[str]:
    ids = set()
    for p in root.iterdir():
        if p.is_dir():
            m = R_ID_PREFIX.match(p.name); 
            if m: ids.add(m.group(1))
    for p in root.rglob("*"):
        if p.is_file():
            m = R_ID_PREFIX.match(p.name); 
            if m: ids.add(m.group(1))
    return sorted(ids, key=int)

def existing_folder_for_id(root: pathlib.Path, pageid: str) -> Optional[pathlib.Path]:
    for p in root.iterdir():
        if p.is_dir() and p.name.startswith(f"{pageid} - "): return p
    return None

def run_covers_from_fs(workers: int) -> None:
    ids = ids_from_fs(ROOT)
    if not ids:
        logging.warning("No IDs found in boothdl/ to refresh covers for."); return
    logging.info(f"Refreshing covers for {len(ids)} item id(s) found in filesystem.")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = []
        for pid in ids:
            folder = existing_folder_for_id(ROOT, pid)
            futs.append(ex.submit(save_primary_image, "", pid, "", folder))
        for _ in tqdm(as_completed(futs), total=len(futs), desc="covers-from-fs"): pass

# ---------- Main ----------
def main(argv: List[str]) -> int:
    import argparse
    t0 = time.perf_counter()

    ap = argparse.ArgumentParser(prog="booth-downloader",
        description="Booth.pm downloader (HTTP/2 pages, streaming downloads, manifest cache).")
    ap.add_argument("--cookies", default="", help="Path to Netscape cookies.txt (fallback)")
    ap.add_argument("--cookie-mode", choices=["auto","file","browser"], default="auto",
                    help="Where to get cookies from first (default: auto)")
    ap.add_argument("--browser", choices=["chrome","edge","brave","firefox","opera"], default=None,
                    help="Use only this browser for cookie lookup (faster).")
    ap.add_argument("--profile", default=os.getenv("BROWSER_PROFILE",""), help="Browser profile (e.g. 'Default')")
    ap.add_argument("--workers", type=int, default=8, help="Parallel workers (recommend: 8-16)")
    ap.add_argument("--pool-size", type=int, default=10, help="HTTP connection pool size (>= workers)")
    ap.add_argument("--req-interval", type=float, default=0.0, help="Min seconds between ANY HTTP requests")
    ap.add_argument("--jitter", type=float, default=0.0, help="Random jitter (+/-) added to req interval")
    ap.add_argument("--batch-size", type=int, default=8, help="Download batch size")
    ap.add_argument("--batch-sleep", type=float, default=0.0, help="Seconds to sleep between download batches")
    ap.add_argument("--forbidden-sleep", type=float, default=300.0, help="Seconds to sleep on HTTP 403")
    ap.add_argument("--max-pages", type=int, default=0, help="Fallback cap if last-page detection fails (0=auto)")
    ap.add_argument("--covers-from-fs", action="store_true", help="Only refresh covers by scanning boothdl/* for IDs")
    ap.add_argument("--no-auth-check", action="store_true", help="Skip the initial auth GET (starts indexing sooner)")
    ap.add_argument("-v", action="count", default=0, help="Verbose output (-v or -vv)")
    ap.add_argument("-vv", action="count", default=0, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    log_setup(args.v + args.vv)
    workers = max(1, int(args.workers or 8))
    pool_size = max(1, int(args.pool_size or 10))
    batch_size = max(1, int(args.batch_size or 8))
    cfg_http(pool_size)

    global RL, FORBIDDEN_SLEEP
    RL = RateLimiter(float(args.req_interval or 0.0), float(args.jitter or 0.0))
    FORBIDDEN_SLEEP = max(0.0, float(args.forbidden_sleep or 300.0))

    # Startup (timed)
    t_stage = time.perf_counter()
    ensure_cookies_targeted(args.cookie_mode, args.browser, args.profile, args.cookies)
    t_cookies = time.perf_counter() - t_stage

    t_stage = time.perf_counter(); _manifest_load(); t_manifest = time.perf_counter() - t_stage
    t_stage = time.perf_counter(); init_http2_client(); t_http2 = time.perf_counter() - t_stage

    if not args.no_auth_check:
        t_stage = time.perf_counter(); auth_ok(); t_auth = time.perf_counter() - t_stage
    else:
        t_auth = 0.0

    logging.info(f"startup: cookies={t_cookies:.2f}s, manifest={t_manifest:.2f}s, http2={t_http2:.2f}s, auth={t_auth:.2f}s")

    if args.covers_from_fs:
        run_covers_from_fs(workers)
        print("\nDone refreshing covers from filesystem IDs.")
        return 0

    # Index first page (HTTP/2)
    r = fetch_html(INDEX); r.raise_for_status()
    from_index = orders_on_index(r.text)
    last = last_page_from_index(r.text) or 1
    if args.max_pages and last > args.max_pages: last = args.max_pages
    pages = [INDEX] + ([f"{INDEX}?page={i}" for i in range(2, last + 1)] if last > 1 else [])

    order_urls = list(dict.fromkeys(from_index))
    if len(pages) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, pool_size)) as ex:
            futs = {ex.submit(fetch_html, u): u for u in pages[1:]}
            for fut in tqdm(as_completed(futs), total=len(futs), desc="index"):
                try:
                    rp = fut.result(); rp.raise_for_status()
                    order_urls += orders_on_index(rp.text)
                except Exception as e:
                    logging.debug(f"index err: {e}")
    order_urls = sorted(set(order_urls))
    logging.info(f"Discovered {len(order_urls)} order(s)")

    if not order_urls:
        logging.error("No order URLs found. Ensure cookies include accounts.booth.pm."); return 2

    # Pull downloadables
    dl_entries: List[Tuple[str, str, str, str]] = []
    with ThreadPoolExecutor(max_workers=min(workers, pool_size)) as ex:
        futs = {ex.submit(fetch_html, u): u for u in order_urls}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="orders"):
            try:
                r2 = fut.result(); r2.raise_for_status()
                soup = soupify(r2.text)
                for a in soup.find_all("a", href=True):
                    m = R_DL.search(a["href"]); 
                    if not m: continue
                    block = _closest_block(a)
                    pid, title, store = ("", "", "")
                    if block: pid, title, store = _find_item_in_block(block)
                    if not pid: pid, title, store = _find_prev_item(a)
                    if not pid: pid, title, store = "unknown", "Unknown Item", ""
                    dl_entries.append((f"https://booth.pm/downloadables/{m.group(1)}", pid, title, store))
            except Exception as e:
                logging.debug(f"order err: {e}")
    logging.info(f"Found {len(dl_entries)} downloadable(s)")

    jobs_dl = [(url, pid, title, store) for (url, pid, title, store) in dl_entries]
    unique_items = {(pid, store, title) for (_u, pid, title, store) in dl_entries if pid and pid != "unknown"}

    w = min(workers, pool_size)
    with ThreadPoolExecutor(max_workers=w) as ex:
        cover_futs = [ex.submit(save_primary_image, store, pid, title) for (pid, store, title) in unique_items]

        i = 0
        for batch in batched(jobs_dl, batch_size):
            i += 1
            dl_futs = [
                ex.submit(
                    stream_download,
                    url,
                    ROOT / f"{pid} - {slug(get_canonical_title(store, pid, title))}",
                    pid,
                ) for (url, pid, title, store) in batch
            ]
            for _ in tqdm(as_completed(dl_futs), total=len(dl_futs), desc=f"download batch {i}"): pass

        for _ in tqdm(as_completed(cover_futs), total=len(cover_futs), desc="cover finalize"): pass

    print("\nAll done. Files are in:", ROOT.resolve())
    logging.info(f"total elapsed: {time.perf_counter()-t0:.2f}s")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv[1:]))
