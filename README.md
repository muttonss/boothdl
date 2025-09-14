# Booth.pm Purchased Items Downloader

Fast, pragmatic downloader for **your purchased items** on Booth.pm.

- Crawls your **Orders** pages at `https://accounts.booth.pm/orders`
- Resolves each item’s **storefront page ID** and title
- Downloads **all files** linked via `/downloadables/{id}`
- Grabs the **first storefront image** (OpenGraph/Twitter image)
- Organizes everything with human-readable names

```
boothdl/
  {pageid} - {item title}/
    {pageid} - {downloaded filename 1}
    {pageid} - {downloaded filename 2}
    {pageid} - {item title}.{jpg|png|webp}   # cover image (first only)
```

> ⚠️ For personal use with content you own. Respect creators’ licenses and Booth.pm’s ToS.

---

## Notable features

- **Accurate naming**  
  Folders and files use the real **storefront page ID** and canonical title.

- **Cover image fetching**  
  Downloads the first cover image from the item page (`og:image` / `twitter:image`).

- **Manifest cache**  
  `_manifest.json` (at script root) remembers what you’ve already downloaded and skips it.

- **Resilient streaming**  
  Atomic `.part` writes + safe renames to avoid “file in use” errors on Windows.

- **Rate-limit avoidance**  
  Backoff on **403/429/503**, optional pacing (`--req-interval`, `--batch-sleep`, `--jitter`).

- **Parallel**  
  Thread-pooled HTML fetch and batched downloads; optional HTTP/2 (via `httpx`) for page fetches.

- **Cookie discovery**  
  Checks `cookies.txt` first, then browser cookies (Chrome/Edge/Brave/Firefox/Opera).  
  Targets **.booth.pm** and **accounts.booth.pm** only.

---

## Install

### 1) Python & deps

```bash
python -m pip install -U pip
pip install requests beautifulsoup4 tqdm browser-cookie3 httpx lxml
```

### 2) Obtaining Cookies

- **Browser cookies (recommended):** stay logged in to Booth in your default browser.
- **cookies.txt file:** export a Netscape-format `cookies.txt` (e.g., using the “Get cookies.txt” extension) and put it next to the script.

---

## Usage

Basic:

```bash
python boothdl.py
```

Verbose:

```bash
python boothdl.py -v
```

Force browser cookie mode (Chrome, profile “Default”):

```bash
python boothdl.py --cookie-mode browser --browser chrome --profile Default -v
```

Cap how many Orders pages to scan:

```bash
python boothdl.py --max-pages 5
```

Fast start (skip auth preflight when you know you’re logged in):

```bash
python boothdl.py --no-auth-check
```

Refresh **only** cover images for items already on disk (scans `boothdl/*` for page IDs):

```bash
python boothdl.py --covers-from-fs
```

---

## Command-line arguments

| Flag | Default | Description |
|---|---:|---|
| `--cookies` | `""` | Path to Netscape `cookies.txt` (fallback if browser cookies aren’t found). |
| `--cookie-mode` | `auto` | `auto`, `file`, or `browser`. Where to look first for cookies. |
| `--browser` | `None` | Force a specific browser for cookie lookup: `chrome`, `edge`, `brave`, `firefox`, `opera`. |
| `--profile` | `""` | Browser profile name (e.g., `Default`). |
| `--workers` | `8` | Max parallel workers (HTML + downloads). |
| `--pool-size` | `10` | HTTP connection pool size (should be ≥ workers). |
| `--req-interval` | `0.0` | Minimum seconds between **any** HTTP requests (global throttle). |
| `--jitter` | `0.0` | ± random jitter added to `--req-interval` (helps avoid patterns). |
| `--batch-size` | `8` | How many file downloads to kick off per batch. |
| `--batch-sleep` | `0.0` | Seconds to sleep between download batches. |
| `--forbidden-sleep` | `300.0` | Cool-down on HTTP 403 before retrying. |
| `--max-pages` | `0` | Hard cap for Orders pagination (0 = auto detect last page). |
| `--covers-from-fs` | off | Only update cover images based on IDs found in `boothdl/`. No downloads. |
| `--no-auth-check` | off | Skip initial auth GET for faster startup. |
| `-v` / `-vv` | off | Verbose / very verbose logging. |

---

## How it works 

1. **Cookies**: loads `cookies.txt` if present, otherwise tries browser cookies (booth.pm & accounts.booth.pm).
2. **Auth preload** *(unless `--no-auth-check`)*: fetches the Orders page to confirm session.
3. **Index**: detects the last Orders page and concurrently fetches all order pages.
4. **Discovery**: parses each order page for `/downloadables/{id}` links and nearby storefront item links.
5. **Cover image**: for each unique item page ID, concurrently fetches the item page and saves the first cover image.
6. **Downloads**: downloads files in batches, using atomic `.part` files and chunked streaming.
7. **Manifest**: after each successful save, `_manifest.json` is updated so reruns skip existing files.

---

## Tips & troubleshooting

- **Rate limiting / 403 / 429:**  
  Increase `--req-interval`, add `--jitter`, reduce `--workers` or `--batch-size`, and/or set `--batch-sleep 3`.

- **“File in use” on Windows:**  
  The script writes to `*.part` and atomically renames; if you still see errors, avoid opening files mid-download and retry.

- **Weird filenames like `%E3%81…`**  
  The script decodes URL-encoded names and sanitizes for Windows reserved characters.

- **Nothing found:**  
  Ensure cookies belong to **accounts.booth.pm** and **.booth.pm**. If you exported cookies, keep only those domains in `cookies.txt` or let the script use your browser profile.

- **Faster startup:**  
  Use `--cookie-mode browser --browser [BROWSER] --profile Default --no-auth-check` if you’re definitely logged in.

---

## Safety & legal

- Use this tool **only** for content you purchased and are permitted to download.  
- Respect creators’ **licenses** and Booth.pm’s **terms of service**.  
- No guarantees: this is best-effort and may break if Booth’s markup changes.

---
