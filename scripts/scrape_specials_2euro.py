#!/usr/bin/env python3
"""
Scrape 2-euro commemorative coin issues by year and export a German-labeled CSV.

This script downloads (and caches) the yearly "Commemorative coins" table from:
    https://www.2euro-overview.eu/index.php?jahr=YYYY

It parses each coin entry and writes a CSV suitable for manual transfer into the
overall Excel spreadsheet (XLSM). I need to copy the result manually in the overall
excel spreadsheet.

Key behaviors:
- Caching: Each requested year is stored as `cache/<YEAR>.html` and reused on reruns.
- Output encoding: CSV is written as UTF-8 with BOM (`utf-8-sig`) for Excel-friendly umlauts.
- Germany mint split: If `Herkunftsland == Germany`, the total mintage is split evenly across
  mints A, D, F, G, J, producing 5 rows per coin (integer-exact; remainder distributed).
- Images: For each coin we try to discover the gbv image token from the page's "showcase"
  area, download https://www.2euro-overview.eu/gbv.php?jlwq=<token> once, and store it as
  <img-dir>/<token>.jpg. The CSV link points to your server base URL + "<token>.jpg".

Usage examples:
    python scrape_specials_2euro.py 2024 -o coins.csv
    python scrape_specials_2euro.py 2020-2024 --cache-dir cache --img-dir images -o coins.csv
    python scrape_specials_2euro.py "2022,2024" --img-base-url http://37.120.179.15:8000/coinbot/ -o coins.csv
"""

import argparse
import csv
import re
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

BASE: str = "https://www.2euro-overview.eu/index.php"
SITE_ROOT: str = "https://www.2euro-overview.eu/"
HDR: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
}

RawRow = Dict[str, Any]
OutRow = Dict[str, str]


_PRIMED: set[int] = set()


def _prime_session(sess: requests.Session, year: int) -> None:
    """Establish cookies/session state for image endpoints (once per year)."""
    if year in _PRIMED:
        return
    hdr = {"User-Agent": "Mozilla/5.0", "Referer": f"{BASE}?jahr={year}"}
    sess.get(f"{BASE}?jahr={year}", headers=hdr, timeout=30).raise_for_status()
    _PRIMED.add(year)


def years(specs: Sequence[str]) -> List[int]:
    """Parse year specifications into a sorted list of unique years.

    Accepts individual years, comma-separated lists, and ranges:
      - "2024"
      - "2022,2023,2024"
      - "2020-2024"
      - mixed: ["2021", "2023-2024", "2020,2022"]

    Args:
        specs: One or more year spec strings.

    Returns:
        Sorted list of unique years.
    """
    ys: set[int] = set()
    for s in specs:
        for p in s.split(","):
            p = p.strip()
            if not p:
                continue
            if "-" in p:
                a, b = map(int, p.split("-", 1))
                ys.update(range(min(a, b), max(a, b) + 1))
            else:
                ys.add(int(p))
    return sorted(ys)


def digits_int(s: Optional[str]) -> Optional[int]:
    """Extract all digits from a string and return as int (or None if empty).

    Examples:
        "2.125.000" -> 2125000
        "0" -> 0
        "" -> None

    Args:
        s: Input string.

    Returns:
        Parsed integer, or None if no digits are present.
    """
    d = re.sub(r"\D+", "", s or "")
    return int(d) if d else None


def normalize_nr(s: str) -> Optional[str]:
    """Normalize coin number strings to a stable 'CC. <n>' form.

    Args:
        s: Text containing a coin number.

    Returns:
        Normalized number (e.g., "CC. 514") or None if not found.
    """
    m = re.search(r"\bCC\.\s*(\d+)\b", s)
    return f"CC. {m.group(1)}" if m else None


def get_html(year: int, sess: requests.Session, cache_dir: Path) -> bytes:
    """Fetch a year page HTML as bytes, using a local cache if available.

    Args:
        year: Year to fetch.
        sess: Requests session.
        cache_dir: Directory for cached HTML files.

    Returns:
        Raw HTML bytes for the given year.
    """
    p = cache_dir / f"{year}.html"
    if p.exists():
        return p.read_bytes()
    r = sess.get(BASE, params={"jahr": year}, timeout=(10, 60))
    r.raise_for_status()
    p.write_bytes(r.content)
    return r.content


def clean_url(u: Optional[str]) -> str:
    """Remove PHPSESSID from a URL to make it stable for storage.

    Args:
        u: Possibly session-decorated URL.

    Returns:
        URL without the PHPSESSID query parameter; empty string if input is None/empty.
    """
    if not u:
        return ""
    s = urlsplit(u)
    q: List[Tuple[str, str]] = [
        (k, v)
        for k, v in parse_qsl(s.query, keep_blank_values=True)
        if k.upper() != "PHPSESSID"
    ]
    return urlunsplit(
        (s.scheme, s.netloc, s.path, urlencode(q, doseq=True), s.fragment)
    )


def fmt_mill(n: Optional[int]) -> str:
    """Format an integer amount as a string in millions.

    Args:
        n: Absolute count (e.g., 2125000).

    Returns:
        Decimal string in millions (e.g., "2.125"), or empty string if n is None/0.
    """
    if not n:
        return ""
    x = n / 1_000_000
    return f"{x:.3f}".rstrip("0").rstrip(".")


def extract_image_tokens(soup: BeautifulSoup) -> Mapping[str, str]:
    """Extract mapping from coin number (e.g., 'CC. 514') to image token.

    The page contains tables with class "showcase". The first row includes <img id="TOKEN">,
    the second row includes labels like 'CC. 514 . France'. We pair them by position.

    Args:
        soup: Parsed BeautifulSoup document.

    Returns:
        Dict mapping normalized coin number -> token string.
    """
    out: Dict[str, str] = {}
    for tbl in soup.find_all("table", class_="showcase"):
        trs = tbl.find_all("tr")
        if len(trs) < 2:
            continue
        imgs = trs[0].find_all("img", id=True)
        labels = trs[1].find_all("b")
        for img, lab in zip(imgs, labels):
            nr = normalize_nr(lab.get_text(" ", strip=True))
            token = img.get("id")
            if nr and token:
                out[nr] = str(token)
    return out


def sniff_is_jpeg(b: bytes) -> bool:
    """Return True if bytes look like a JPEG file (JFIF/EXIF)."""
    return b.startswith(b"\xff\xd8\xff")


def download_gbv_jpg(
    image_id: str,
    year: int,
    sess: requests.Session,
    img_dir: Path,
) -> Optional[Path]:
    """Download a gbv image as <image_id>.jpg if not already present.

    Args:
        image_id: gbv token (value of 'jlwq').
        year: Year page used as HTTP referer (helps server-side checks).
        sess: Requests session.
        img_dir: Directory to store downloaded images.

    Returns:
        Path to the saved JPG, or None if the response is not a JPEG.
    """
    out = img_dir / f"{image_id}.jpg"
    if out.exists():
        return out

    _prime_session(sess, year)

    r = sess.get(
        urljoin(SITE_ROOT, "gbv.php"),
        params={"jlwq": image_id},
        headers={"User-Agent": "Mozilla/5.0", "Referer": f"{BASE}?jahr={year}"},
        timeout=(10, 60),
    )
    r.raise_for_status()
    b = r.content
    if not sniff_is_jpeg(b):
        print("Could not download", image_id)
        return None  # keep it strict: you explicitly want JPG
    out.write_bytes(b)
    sleep(0.5)
    return out


def parse_year(year: int, html: bytes) -> List[RawRow]:
    """Parse the 'Commemorative coins' table plus gbv image IDs from a year page.

    Args:
        year: Year corresponding to the page.
        html: Raw HTML bytes.

    Returns:
        List of rows with keys:
            year, nr, country, subject, date, bu, proof, provisional, details_url, image_id
    """
    soup = BeautifulSoup(html, "html.parser", from_encoding="ISO-8859-1")
    img_tokens = extract_image_tokens(soup)

    h2 = soup.find(
        lambda t: isinstance(t, Tag)
        and t.name in ("h2", "h1")
        and "Commemorative coins" in t.get_text()
    )
    table = h2.find_next("table") if h2 else soup.find("table")
    if not table:
        return []

    out: List[RawRow] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        nr_raw = tds[0].get_text(" ", strip=True)
        nr = normalize_nr(nr_raw) or nr_raw.strip()
        if "CC." not in nr:
            continue

        a = tr.find("a", href=True)
        out.append(
            {
                "year": year,
                "nr": nr,
                "country": tds[2].get_text(" ", strip=True),
                "subject": tds[3].get_text(" ", strip=True),
                "date": tds[4].get_text(" ", strip=True),
                "bu": digits_int(tds[5].get_text(" ", strip=True)),
                "proof": digits_int(tds[6].get_text(" ", strip=True)),
                "provisional": (
                    "*" in (tds[7].get_text(" ", strip=True) if len(tds) > 7 else "")
                ),
                "details_url": urljoin(BASE, a["href"]) if a else None,
                "image_id": img_tokens.get(nr),
            }
        )
    return out


def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point."""
    ap = argparse.ArgumentParser()
    ap.add_argument("years", nargs="+", help='e.g. 2024 or "2020-2024" or "2022,2024"')
    ap.add_argument("-o", "--out", default="coins.csv", help="Output CSV path.")
    ap.add_argument(
        "--cache-dir",
        default="2euro-overview/sites",
        help="Directory for cached HTML files.",
    )
    ap.add_argument(
        "--img-dir",
        default="2euro-overview/imgs",
        help="Directory for downloaded JPGs.",
    )
    ap.add_argument(
        "--img-base-url",
        default="http://37.120.179.15:8000/coinbot/",
        help="Base URL for CSV links (files will be <base>/<image_id>.jpg).",
    )
    args = ap.parse_args(argv)

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(exist_ok=True)

    img_dir = Path(args.img_dir)
    img_dir.mkdir(exist_ok=True)

    img_base = args.img_base_url.rstrip("/") + "/"

    sess = requests.Session()
    sess.headers.update(HDR)

    rows: List[RawRow] = []
    for y in years(args.years):
        rows += parse_year(y, get_html(y, sess, cache_dir))

    # Download images (once), skipping already-present files.
    for r in tqdm(rows, desc="Downloading images", total=len(rows)):
        image_id = r.get("image_id")
        # breakpoint()
        if image_id:
            download_gbv_jpg(str(image_id), int(r["year"]), sess, img_dir)

    def base_row(r: RawRow, amount_int: int, mint: str, per_country: bool) -> OutRow:
        """Build one output row in the target schema."""
        image_id = str(r.get("image_id") or "")
        return {
            "Name der Münze": str(r["subject"]),
            "Herkunftsland": str(r["country"]),
            "Ausgabejahr": str(r["year"]),
            "Menge in Mill.": fmt_mill(amount_int),
            "Landspezifisch?": "True" if per_country else "False",
            "Prägestätte": mint,
            "Wert": "2 euro",
            "Link": f"{img_base}{image_id}.jpg" if image_id else "",
            "Beschreibung": str(r["subject"]),
        }

    out_rows: List[OutRow] = []
    for r in rows:
        total = int((r.get("bu") or 0) + (r.get("proof") or 0))
        if r.get("country") == "Germany" and total:
            mints = ["A", "D", "F", "G", "J"]
            q, rem = divmod(total, len(mints))
            for i, m in enumerate(mints):
                out_rows.append(base_row(r, q + (1 if i < rem else 0), m, True))
        else:
            out_rows.append(base_row(r, total, "", False))

    out_rows.sort(
        key=lambda rr: (
            rr["Herkunftsland"],
            rr["Name der Münze"],
            rr["Prägestätte"] or "ZZZ",
        )
    )

    fields: List[str] = [
        "Name der Münze",
        "Herkunftsland",
        "Ausgabejahr",
        "Menge in Mill.",
        "Landspezifisch?",
        "Prägestätte",
        "Wert",
        "Link",
        "Beschreibung",
    ]
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)


if __name__ == "__main__":
    main()
