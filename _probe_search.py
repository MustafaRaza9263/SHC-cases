import re
import socket
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

real = socket.getaddrinfo


def fake(host, port, family=0, type=0, proto=0, flags=0):
    if host == "cases.shc.gov.pk":
        try:
            return real(host, port, family, type, proto, flags)
        except OSError:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))
                for ip in ("172.67.210.235", "104.21.77.178")
            ]
    return real(host, port, family, type, proto, flags)


socket.getaddrinfo = fake
s = requests.Session()
s.headers["User-Agent"] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"
)

base = "https://cases.shc.gov.pk/khi/web/index.php"
params = {
    "r": "cases/search-result",
    "CasesSearch[CASENO]": "",
    "CasesSearch[CASEYEAR]": "",
    "CasesSearch[CASENAMECODE]": "",
    "CasesSearch[BENCH]": "",
    "CasesSearch[CIRCUITCODE]": "",
    "CasesSearch[MATTERCODE]": "",
    "CasesSearch[PARTY]": "",
    "CasesSearch[GOVT_AGENCY_CODE]": "",
    "CasesSearch[FIRNO]": "",
    "CasesSearch[FIRYEAR]": "",
    "CasesSearch[POLICESTATIONCODE]": "",
    "CasesSearch[ADVOCATECODE]": "",
    "CasesSearch[isPending]": "3",
}

# page 1
r = s.get(base, params=params, timeout=120)
print("page1 status", r.status_code, "len", len(r.text), "final", r.url)
Path(r"d:\scraper\samples\search_p1.html").write_text(r.text, encoding="utf-8")

soup = BeautifulSoup(r.text, "lxml")
summary = soup.select_one(".summary")
print("summary:", summary.get_text(" ", strip=True) if summary else None)

# pager links
pager_hrefs = []
for a in soup.select(".pagination a, .kv-panel-pager a, ul.pagination a"):
    href = a.get("href") or ""
    text = a.get_text(strip=True)
    if href:
        pager_hrefs.append((text, href[:200]))
print("pager samples:", pager_hrefs[:15])

# table headers + first rows
table = soup.select_one("table.kv-grid-table, table.table")
if table:
    headers = [th.get_text(" ", strip=True) for th in table.select("thead th")]
    print("headers:", headers)
    for i, tr in enumerate(table.select("tbody tr")[:3]):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        links = [a.get("href") for a in tr.select("a[href]")]
        print(f"row{i}:", cells)
        print(f" links{i}:", links)

# detect page/per-page query keys
page_keys = sorted(set(re.findall(r"[?&]([a-zA-Z0-9_\[\]%-]*page[a-zA-Z0-9_\[\]%-]*)=", r.text, re.I)))
per_keys = sorted(set(re.findall(r"[?&]([a-zA-Z0-9_\[\]%-]*per[-_]?page[a-zA-Z0-9_\[\]%-]*)=", r.text, re.I)))
print("page keys", page_keys)
print("per-page keys", per_keys)

# try page 2 and larger page size
time.sleep(1)
p2 = dict(params)
# common yii: page=2
for key, val in [("page", "2"), ("per-page", "50")]:
    pass

# extract literal page= from first next link
next_link = None
for text, href in pager_hrefs:
    if text in {"2", "›", "»", "Next", "next"} or "page=" in href:
        next_link = href
        if text == "2":
            break
print("chosen next", next_link)

if next_link:
    url = urljoin(base, next_link.replace("&amp;", "&"))
    time.sleep(1)
    r2 = s.get(url, timeout=120)
    print("page2 status", r2.status_code, "len", len(r2.text), "url", r2.url[:180])
    Path(r"d:\scraper\samples\search_p2.html").write_text(r2.text, encoding="utf-8")
    soup2 = BeautifulSoup(r2.text, "lxml")
    print("summary2:", (soup2.select_one(".summary").get_text(" ", strip=True) if soup2.select_one(".summary") else None))

# try per-page=100 on page 1
time.sleep(1)
pbig = dict(params)
pbig["per-page"] = "100"
pbig["page"] = "1"
rb = s.get(base, params=pbig, timeout=120)
print("per-page100 status", rb.status_code, "len", len(rb.text))
sb = BeautifulSoup(rb.text, "lxml")
print("summary100:", (sb.select_one(".summary").get_text(" ", strip=True) if sb.select_one(".summary") else None))
tb = sb.select_one("table.kv-grid-table, table.table")
if tb:
    print("rows100", len(tb.select("tbody tr")))
