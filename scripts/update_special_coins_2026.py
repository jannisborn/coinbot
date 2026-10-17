#!/usr/bin/env python3
"""Scrape Muenzen.eu's 2026 commemorative coins and append them to coins.xlsm."""

import csv
import re
from copy import copy
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

URL = "https://www.muenzen.eu/2-euro-muenzen-2026.html"
HEADERS = [
    "Name der Münze", "Herkunftsland", "Ausgabejahr", "Menge in Mill.",
    "Landspezifisch?", "Prägestätte", "Wert", "Link", "Details",
]
COUNTRIES = {
    "Deutschland": "Germany", "Slowakei": "Slovakia", "Frankreich": "France",
    "Italien": "Italy", "San Marino": "San Marino", "Malta": "Malta",
    "Zypern": "Cyprus", "Estland": "Estonia", "Griechenland": "Greece",
    "Belgien": "Belgium", "Monaco": "Monaco", "Slowenien": "Slovenia",
    "Irland": "Ireland", "Luxemburg": "Luxembourg", "Portugal": "Portugal",
    "Kroatien": "Croatia", "Lettland": "Latvia", "Bulgarien": "Bulgaria",
    "Finnland": "Finland", "Vatikan": "Vatican", "Litauen": "Lithuania",
}
GERMAN_MINTAGES = {"A": 6.0, "D": 6.3, "F": 7.2, "G": 4.2, "J": 6.3}


def scrape():
    response = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = next(t for t in soup.find_all("table") if "Münze" in t.get_text())
    coins = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 5 or not cells[0].get_text(strip=True):
            continue
        name = cells[0].get_text(" ", strip=True)
        country = cells[3].get_text(" ", strip=True)
        if country not in COUNTRIES:
            continue
        amount = int(re.sub(r"\D", "", cells[4].get_text())) / 1_000_000
        img = cells[1].find("img")
        coins.append({
            "Name der Münze": name,
            "Herkunftsland": COUNTRIES[country],
            "Ausgabejahr": 2026,
            "Menge in Mill.": amount,
            "Landspezifisch?": False,
            "Prägestätte": "",
            "Wert": "2 euro",
            "Link": urljoin(URL, img.get("data-src") or img.get("src", "")) if img else "",
            "Details": "",
        })
    expanded = []
    for coin in coins:
        if coin["Herkunftsland"] == "Germany":
            for mint, amount in GERMAN_MINTAGES.items():
                expanded.append({**coin, "Menge in Mill.": amount, "Prägestätte": mint})
        else:
            expanded.append(coin)
    return sorted(expanded, key=lambda c: (c["Herkunftsland"].casefold(), c["Name der Münze"].casefold(), c["Prägestätte"]))


def update_workbook(path, coins):
    wb = load_workbook(path, keep_vba=True)
    ws = wb["Sondermünzen"]
    rows = [r for r in range(2, ws.max_row + 1) if str(ws.cell(r, 3).value) == "2026"]
    start = min(rows) if rows else ws.max_row + 1
    template = start - 1
    old_end = max(rows, default=start - 1)
    end = max(old_end, start + len(coins) - 1)
    for row in range(start, end + 1):
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row, col)
            cell.value = None
            source = ws.cell(template, col)
            if source.has_style:
                cell._style = copy(source._style)
    for offset, coin in enumerate(coins):
        for col, header in enumerate(HEADERS, 1):
            value = coin[header]
            if header == "Ausgabejahr":
                value = int(value)
            elif header == "Menge in Mill.":
                value = float(value)
            elif header == "Landspezifisch?" and isinstance(value, str):
                value = value.casefold() == "true"
            ws.cell(start + offset, col, value)
    wb.save(path)
    return len(coins)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    coins = scrape()
    with (root / "data" / "special_coins_2026.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(coins)
    print(f"Saved {len(coins)} entries; added {update_workbook(root / 'coins.xlsm', coins)} workbook rows.")
