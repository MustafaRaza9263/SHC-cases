#!/usr/bin/env python3
"""
Sindh High Court (cases.shc.gov.pk) polite, resumable case scraper.

Exports:
  - data/cases.jsonl          (append-only checkpoint of scraped cases)
  - data/checkpoint.json      (progress per circuit)
  - data/cases.json           (final nested JSON, written on export)
  - data/cases.xlsx           (cases / hearings / advocates sheets)

Speed note:
  Most delay is the origin/Cloudflare response time (~several seconds per
  page). The built-in delay is polite pacing so a long run is less likely
  to be blocked; it is not inventing slowness the server does not already have.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_HOST = "https://cases.shc.gov.pk"

# Circuit slug -> human label. IDs are NOT shared across circuits.
CIRCUITS: dict[str, str] = {
    "khi": "Karachi",
    "hyd": "Hyderabad",
    "suk": "Sukkur",
    "lar": "Larkana",
    "mpkhas": "Mirpurkhas",
}   

# Fallback Cloudflare edge IPs if local DNS flakes.
FALLBACK_IPS = ("172.67.210.235", "104.21.77.178")

DEFAULT_DELAY = 0.0
DEFAULT_JITTER = 0.0
REQUEST_TIMEOUT = 90
MAX_RETRIES = 4
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

NOT_SET_RE = re.compile(r"^\s*\(not set\)\s*$", re.I)
CASE_NO_RE = re.compile(
    r"^(?P<case_type>.+?)\s+"
    r"(?:\((?P<bench_flag>[SD])\)\s+)?"
    r"(?P<case_number>\d+)\s*/\s*(?P<case_year>\d{4})\s+"
    r"(?P<court_location>[A-Za-z ]+?)"
    r"(?:\s*\((?P<nature>[^)]*)\))?\s*$",
    re.I,
)
ADVOCATE_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<ledger>[^)]+)\)\s*$")
PROFILE_HEAD_RE = re.compile(
    r"Case Profile\s*\((?P<case_id>\d*)\)\s*(?P<status>.*)$", re.I
)

TAB_ROUTES = {
    "connected": ("case-attach/index", "casecode"),
    "hearings": ("cases/causelist-history", "casecode"),
    "parties": ("case-parties/index", "casecode"),
    "applications": ("cma/index", "casecode"),
    "advocates": ("cases/advocate-detail", "casecode"),
    "discharge": ("cases/discharge-history", "casecode"),
    "aros": ("cases/aros-history", "casecode"),
    "govt_depts": ("cases/org-detail", "casecode"),
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path, verbose: bool = False) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("shc_scraper")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_dir / "scraper.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    return logger


log = logging.getLogger("shc_scraper")


# ---------------------------------------------------------------------------
# HTTP client (DNS failover + polite pacing)
# ---------------------------------------------------------------------------

def install_dns_failover(host: str = "cases.shc.gov.pk") -> None:
    """Prefer system DNS; fall back to known Cloudflare A records."""
    real_getaddrinfo = socket.getaddrinfo

    def patched(hostname, port, family=0, type=0, proto=0, flags=0):
        if hostname != host:
            return real_getaddrinfo(hostname, port, family, type, proto, flags)
        try:
            return real_getaddrinfo(hostname, port, family, type, proto, flags)
        except OSError:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))
                for ip in FALLBACK_IPS
            ]

    socket.getaddrinfo = patched  # type: ignore[assignment]


@dataclass
class PoliteSession:
    delay: float = DEFAULT_DELAY
    jitter: float = DEFAULT_JITTER
    timeout: float = REQUEST_TIMEOUT
    _last_request_at: float = field(default=0.0, init=False)
    session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self) -> None:
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
        )

    def _pace(self) -> None:
        if self.delay <= 0 and self.jitter <= 0:
            return
        if self._last_request_at <= 0:
            return
        wait_for = self.delay + random.uniform(0, self.jitter)
        elapsed = time.monotonic() - self._last_request_at
        remaining = wait_for - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """GET with polite pacing.

        Retries only transient failures (timeouts / connection drops / 429 / 503).
        Hard HTTP errors like 500/404 fail immediately — common on broken SHC tabs.
        """
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            self._pace()
            try:
                resp = self.session.get(url, timeout=self.timeout, **kwargs)
                self._last_request_at = time.monotonic()
                if resp.status_code in {429, 503, 520, 521, 522, 523, 524}:
                    backoff = min(60, self.delay * (2 ** attempt)) + random.uniform(0, 3)
                    log.warning(
                        "HTTP %s on %s (attempt %s/%s); backing off %.1fs",
                        resp.status_code,
                        url,
                        attempt,
                        MAX_RETRIES,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                # Origin 5xx/4xx on this portal are usually permanent for that URL
                # (e.g. case-attach 500 on early casecodes). Do not burn minutes retrying.
                if resp.status_code >= 400:
                    resp.raise_for_status()
                return resp
            except requests.HTTPError:
                raise
            except requests.RequestException as exc:
                last_err = exc
                backoff = min(60, self.delay * (2 ** (attempt - 1))) + random.uniform(0, 2)
                log.warning(
                    "Request failed (%s) attempt %s/%s; retry in %.1fs",
                    exc,
                    attempt,
                    MAX_RETRIES,
                    backoff,
                )
                time.sleep(backoff)
        assert last_err is not None
        raise last_err


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Tag):
        text = value.get_text(" ", strip=True)
    else:
        text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("\xa0", " ").strip()
    if NOT_SET_RE.match(text):
        return ""
    return text


def normalize_html_payload(raw: str) -> str:
    """Tab endpoints often return a JSON-encoded HTML string."""
    text = raw.strip()
    if not text:
        return ""
    if text[0] == '"' and text[-1] == '"':
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")
    return text


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(normalize_html_payload(html), "lxml")


def parse_kv_grid_tables(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Parse Yii/Kartik grid tables into list of {header: value} dicts."""
    rows: list[dict[str, str]] = []
    for table in soup.select("table.kv-grid-table"):
        headers = [clean_text(th) for th in table.select("thead th")]
        if not headers:
            # some org tables omit thead
            continue
        for tr in table.select("tbody tr"):
            if tr.select_one("div.empty"):
                continue
            cells = tr.find_all("td", recursive=False)
            if not cells:
                continue
            values = [clean_text(td) for td in cells]
            if len(values) < len(headers):
                values += [""] * (len(headers) - len(values))
            row = {headers[i]: values[i] for i in range(len(headers))}
            if any(row.values()):
                rows.append(row)
    return rows


def parse_labeled_blocks(html_fragment: str) -> dict[str, str]:
    """Parse '<b>Label:</b> value<br>...' style hearing blocks."""
    soup = BeautifulSoup(html_fragment, "lxml")
    out: dict[str, str] = {}
    for bold in soup.find_all("b"):
        label = clean_text(bold).rstrip(":").strip()
        if not label:
            continue
        bits: list[str] = []
        for sib in bold.next_siblings:
            if getattr(sib, "name", None) in {"br", "b"}:
                break
            if getattr(sib, "name", None) == "span":
                bits.append(clean_text(sib))
            else:
                bits.append(clean_text(sib))
        out[label.lower()] = " ".join(x for x in bits if x).strip()
    # also keep a flattened plain text dump
    out["_raw"] = clean_text(soup)
    return out


# ---------------------------------------------------------------------------
# Profile / tab parsers
# ---------------------------------------------------------------------------

def parse_case_number(raw: str) -> dict[str, str]:
    raw = clean_text(raw)
    # Case no often has nature on next line already flattened
    m = CASE_NO_RE.match(raw)
    if not m:
        # try splitting nature if present as trailing (...)
        nature = ""
        body = raw
        nm = re.search(r"\(([^)]+)\)\s*$", raw)
        if nm and not re.search(r"\([SD]\)", nm.group(0)):
            nature = nm.group(1).strip()
            body = raw[: nm.start()].strip()
            m = CASE_NO_RE.match(body + (f" ({nature})" if nature else ""))
        if not m:
            return {
                "case_no_raw": raw,
                "case_type": "",
                "bench_flag": "",
                "case_number": "",
                "case_year": "",
                "court_location": "",
                "nature_of_case": nature,
            }
    return {
        "case_no_raw": raw,
        "case_type": (m.group("case_type") or "").strip(),
        "bench_flag": (m.group("bench_flag") or "").strip().upper(),
        "case_number": (m.group("case_number") or "").strip(),
        "case_year": (m.group("case_year") or "").strip(),
        "court_location": (m.group("court_location") or "").strip(),
        "nature_of_case": (m.group("nature") or "").strip(),
    }


def split_title_parties(title: str) -> tuple[str, str]:
    parts = re.split(r"\bVS\b", title, maxsplit=1, flags=re.I)
    if len(parts) == 2:
        return parts[0].strip(" -"), parts[1].strip(" -")
    return title.strip(), ""


def parse_dates_blob(blob: str) -> dict[str, str]:
    """'11-JAN-23 / / <disposal>/ <consigned>' style field."""
    text = blob.replace("\n", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = clean_text(text)
    bits = [b.strip() for b in text.split("/")]
    while len(bits) < 4:
        bits.append("")
    return {
        "institution_date": bits[0],
        "admit_date": bits[1],
        "disposal_date": bits[2],
        "consigned_date": bits[3],
    }


def parse_stay_blob(blob: str) -> dict[str, str]:
    text = clean_text(blob)
    if not text:
        return {"stay_grant_date": "", "stay_end_date": ""}
    bits = [b.strip() for b in text.split("/")]
    return {
        "stay_grant_date": bits[0] if bits else "",
        "stay_end_date": bits[1] if len(bits) > 1 else "",
    }


def parse_fir_blob(blob: str) -> dict[str, str]:
    text = clean_text(blob)
    if not text:
        return {"fir_no": "", "fir_year": ""}
    m = re.match(r"^(?P<no>\d+)\s*/\s*(?P<year>\d{2,4})$", text)
    if m:
        return {"fir_no": m.group("no"), "fir_year": m.group("year")}
    return {"fir_no": text, "fir_year": ""}


def th_map_from_profile(soup: BeautifulSoup) -> dict[str, Tag]:
    """Map label text -> value <td> for kv-child-table rows."""
    mapping: dict[str, Tag] = {}
    for th in soup.select("table.kv-child-table th"):
        label = clean_text(th)
        # normalize multi-line labels
        label = re.sub(r"\s+", " ", label)
        td = th.find_next_sibling("td")
        if td is not None:
            mapping[label] = td
    return mapping


def parse_profile(html: str, circuit: str, case_id: int) -> dict[str, Any] | None:
    soup = soupify(html)
    heading = soup.select_one("h3.panel-title")
    if not heading or "Case Profile" not in heading.get_text():
        # empty / wrong id often still returns a shell page
        if "Case Profile" not in soup.get_text():
            return None

    status = ""
    label = heading.select_one("span.label") if heading else None
    if label:
        status = clean_text(label)
    head_text = clean_text(heading)
    m = PROFILE_HEAD_RE.search(head_text)
    profile_id_missing = False
    if m and m.group("case_id"):
        found_id = int(m.group("case_id"))
    else:
        # SHC returns "Case Profile ()" for some early/orphan IDs.
        found_id = case_id
        profile_id_missing = True
    if m and m.group("status") and not status:
        status = clean_text(m.group("status"))

    fields = th_map_from_profile(soup)
    remarks_td = soup.select_one("table.detail-view > tr > td .kv-attribute")
    remarks = clean_text(remarks_td) if remarks_td else ""

    def field_html(label: str) -> str:
        td = fields.get(label)
        return str(td) if td else ""

    def field_text(label: str) -> str:
        td = fields.get(label)
        return clean_text(td) if td else ""

    case_no_raw = field_text("Case No.")
    connected_summary = ""
    if "Connected with" in case_no_raw:
        case_no_raw, connected_summary = re.split(
            r"Connected with\s*:?", case_no_raw, maxsplit=1, flags=re.I
        )
        case_no_raw = case_no_raw.strip(" -|")
        connected_summary = connected_summary.strip()

    # Early orphan rows often dump leftover "Appeal filed ..." text into Case No.
    appeal_note = ""
    appeal_m = re.search(r"\bAppeal filed\b(.*)$", case_no_raw, flags=re.I)
    if appeal_m:
        appeal_note = ("Appeal filed" + appeal_m.group(1)).strip()
        case_no_raw = case_no_raw[: appeal_m.start()].strip(" -/|()")
        if remarks:
            remarks = f"{remarks} | {appeal_note}"
        else:
            remarks = appeal_note

    # nature may already be in case_no_raw via <br>
    parsed_no = parse_case_number(case_no_raw)

    dates = parse_dates_blob(field_html("Institution / Admit Date/ Disposal / Consigned Date"))
    # label in HTML is broken across tags; try fuzzy keys
    if not any(dates.values()):
        for key in list(fields):
            if "Institution" in key and "Admit" in key:
                dates = parse_dates_blob(str(fields[key]))
                break

    title = field_text("Case Title")
    side_a, side_b = split_title_parties(title)
    stay = parse_stay_blob(field_text("Stay Grant/End Date"))

    last_h = parse_labeled_blocks(field_html("Last Hearing Detail"))
    next_h = parse_labeled_blocks(field_html("Next Hearing Detail"))
    roster = parse_labeled_blocks(field_html("Last Roster Discharged"))

    fir = parse_fir_blob(field_text("FIR No & Year"))
    undersections = field_text("Undersection(s)")

    case: dict[str, Any] = {
        "circuit": circuit,
        "circuit_name": CIRCUITS.get(circuit, circuit),
        "case_id": found_id,
        "profile_id_missing": profile_id_missing,
        "status": status,
        "remarks": remarks,
        **parsed_no,
        "connected_with_summary": connected_summary,
        "case_title": title,
        "party_side_a": side_a,
        "party_side_b": side_b,
        **dates,
        **stay,
        "last_hearing_date": last_h.get("date", ""),
        "last_hearing_list": last_h.get("list", ""),
        "last_hearing_sr": last_h.get("@ sr #", last_h.get("sr #", "")),
        "last_hearing_stage": last_h.get("stage", ""),
        "last_hearing_bench": last_h.get("bench", ""),
        "last_hearing_other_info": last_h.get("other info", ""),
        "last_hearing_raw": last_h.get("_raw", ""),
        "next_hearing_date": next_h.get("date", ""),
        "next_hearing_list": next_h.get("list", ""),
        "next_hearing_sr": next_h.get("@ sr #", next_h.get("sr #", "")),
        "next_hearing_stage": next_h.get("stage", ""),
        "next_hearing_bench": next_h.get("bench", ""),
        "next_hearing_other_info": next_h.get("other info", ""),
        "next_hearing_raw": next_h.get("_raw", ""),
        "last_roster_discharged_date": roster.get("date", ""),
        "last_roster_discharge_reason": roster.get("reason", ""),
        "future_fixation": field_text("Future Fixation(Tentative)"),
        "last_hg_adlreg": field_text("Last Hg(AdlReg OS/Dy Ex/Nazir)"),
        "next_hg_adlreg": field_text("Next Hg(AdlReg OS/Dy Ex/Nazir)"),
        **fir,
        "fir_sections": undersections,
        "disposal_sine_die": field_text("Disposal Date / Sine Die"),
        "disposing_bench": field_text("Disposing Bench"),
        "nature_of_disposal": field_text("Nature of Disposal"),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "hearings": [],
        "advocates": [],
        "parties": [],
        "applications": [],
        "connected_cases": [],
        "govt_depts": [],
        "discharge_history": [],
        "aros_history": [],
    }
    return case


def parse_hearings(html: str) -> list[dict[str, str]]:
    rows = parse_kv_grid_tables(soupify(html))
    out = []
    for row in rows:
        out.append(
            {
                "hearing_date": row.get("DATE", ""),
                "list": row.get("LIST", ""),
                "sr_no": row.get("SR #", ""),
                "cma": row.get("CMA", ""),
                "other_info": row.get("OTHER INFO", ""),
                "part_heard": row.get("PART HEARD", ""),
                "stage": row.get("STAGE", ""),
                "bench": row.get("BENCH", ""),
                "status": row.get("STATUS", ""),
                "diary": row.get("DIARY", ""),
            }
        )
    return out


def parse_advocates(html: str) -> list[dict[str, str]]:
    soup = soupify(html)
    out: list[dict[str, str]] = []
    for panel in soup.select("div.panel.panel-primary"):
        title = clean_text(panel.select_one("h3.panel-title"))
        side = "unknown"
        if "Appellant" in title or "Petitioner" in title or "Applicant" in title:
            side = "appellant"
        elif "Respondent" in title:
            side = "respondent"
        table = panel.select_one("table.kv-grid-table")
        if not table:
            continue
        for tr in table.select("tbody tr"):
            if tr.select_one("div.empty"):
                continue
            cells = [clean_text(td) for td in tr.find_all("td", recursive=False)]
            if not cells:
                continue
            name_ledger = cells[0]
            entry_date = cells[1] if len(cells) > 1 else ""
            name, ledger = name_ledger, ""
            m = ADVOCATE_RE.match(name_ledger)
            if m:
                name, ledger = m.group("name").strip(), m.group("ledger").strip()
            out.append(
                {
                    "side": side,
                    "advocate_name": name,
                    "ledger_no": ledger,
                    "entry_date": entry_date,
                }
            )
    return out


def parse_parties(html: str) -> list[dict[str, str]]:
    rows = parse_kv_grid_tables(soupify(html))
    return [
        {
            "seq": row.get("#", ""),
            "party_name": row.get("Party Name", ""),
        }
        for row in rows
        if row.get("Party Name")
    ]


def parse_applications(html: str) -> list[dict[str, str]]:
    rows = parse_kv_grid_tables(soupify(html))
    return [
        {
            "seq": row.get("#", ""),
            "appln_no": row.get("Appln No", ""),
            "appln_year": row.get("Appln Year", ""),
            "cma_type": row.get("Cma Type Name", row.get("Cma  Type  Name", "")),
            "applicant_name": row.get("Applicant Name", ""),
        }
        for row in rows
    ]


def parse_connected(html: str) -> list[dict[str, str]]:
    rows = parse_kv_grid_tables(soupify(html))
    return [
        {
            "seq": row.get("#", ""),
            "case_no": row.get("Case No", ""),
            "case_title": row.get("Case Title", ""),
            "last_hearing": row.get("Last Hearing", ""),
            "next_date": row.get("Next Date", ""),
        }
        for row in rows
    ]


def parse_govt_depts(html: str) -> list[dict[str, str]]:
    soup = soupify(html)
    out: list[dict[str, str]] = []
    for panel in soup.select("div.panel.panel-primary"):
        title = clean_text(panel.select_one("h3.panel-title"))
        side = "appellant" if "Appellant" in title else "respondent" if "Respondent" in title else "unknown"
        for tr in panel.select("table.kv-grid-table tbody tr"):
            if tr.select_one("div.empty"):
                continue
            cells = [clean_text(td) for td in tr.find_all("td")]
            if not cells:
                continue
            out.append({"side": side, "dept_name": cells[0], "extra": " | ".join(cells[1:])})
    return out


def parse_simple_history(html: str) -> list[dict[str, str]]:
    return parse_kv_grid_tables(soupify(html))


# ---------------------------------------------------------------------------
# Scraper orchestration
# ---------------------------------------------------------------------------

class SHCScraper:
    def __init__(
        self,
        data_dir: Path,
        delay: float = DEFAULT_DELAY,
        jitter: float = DEFAULT_JITTER,
        fetch_tabs: bool = True,
    ) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.data_dir / "cases.jsonl"
        self.checkpoint_path = self.data_dir / "checkpoint.json"
        self.fetch_tabs = fetch_tabs
        self.http = PoliteSession(delay=delay, jitter=jitter)
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self) -> dict[str, Any]:
        if self.checkpoint_path.exists():
            return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        return {
            "circuits": {
                c: {"last_completed_id": 0, "completed_ids": [], "missing_ids": [], "errors": {}}
                for c in CIRCUITS
            },
            "updated_at": None,
        }

    def save_checkpoint(self) -> None:
        self.checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp = self.checkpoint_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.checkpoint, indent=2), encoding="utf-8")
        tmp.replace(self.checkpoint_path)

    def already_done(self, circuit: str, case_id: int) -> bool:
        done = set(self.checkpoint["circuits"][circuit].get("completed_ids", []))
        return case_id in done

    def mark_done(self, circuit: str, case_id: int, missing: bool = False) -> None:
        cstate = self.checkpoint["circuits"][circuit]
        cstate["last_completed_id"] = max(int(cstate.get("last_completed_id") or 0), case_id)
        ids = set(cstate.get("completed_ids", []))
        ids.add(case_id)
        # keep checkpoint file from growing forever: store only recent window + last id
        # Full resume uses jsonl; completed_ids is a fast skip set for current run.
        cstate["completed_ids"] = sorted(ids)[-5000:]
        if missing:
            missing_ids = set(cstate.get("missing_ids", []))
            missing_ids.add(case_id)
            cstate["missing_ids"] = sorted(missing_ids)[-5000:]
        self.save_checkpoint()

    def mark_error(self, circuit: str, case_id: int, message: str) -> None:
        cstate = self.checkpoint["circuits"][circuit]
        errors = cstate.setdefault("errors", {})
        errors[str(case_id)] = message[:500]
        self.save_checkpoint()

    def append_case(self, case: dict[str, Any]) -> None:
        with self.jsonl_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(case, ensure_ascii=False) + "\n")

    def case_url(self, circuit: str, case_id: int) -> str:
        return f"{BASE_HOST}/{circuit}/web/index.php?r=cases%2Fview&id={case_id}"

    def tab_url(self, circuit: str, route: str, case_id: int, param: str) -> str:
        return f"{BASE_HOST}/{circuit}/web/index.php?r={route.replace('/', '%2F')}&{param}={case_id}"

    def fetch_case(self, circuit: str, case_id: int) -> dict[str, Any] | None:
        url = self.case_url(circuit, case_id)
        log.info("Fetching profile %s/%s", circuit, case_id)
        resp = self.http.get(url)
        case = parse_profile(resp.text, circuit, case_id)
        if case is None:
            log.info("No case profile at %s/%s (empty/missing)", circuit, case_id)
            return None

        if self.fetch_tabs:
            for key, (route, param) in TAB_ROUTES.items():
                tab_url = self.tab_url(circuit, route, case_id, param)
                try:
                    log.debug("Fetching tab %-12s %s/%s", key, circuit, case_id)
                    tab_html = self.http.get(tab_url).text
                except requests.HTTPError as exc:
                    code = exc.response.status_code if exc.response is not None else "?"
                    log.warning(
                        "Tab %s HTTP %s for %s/%s — skipping tab (no retry)",
                        key,
                        code,
                        circuit,
                        case_id,
                    )
                    continue
                except requests.RequestException as exc:
                    log.warning("Tab %s failed for %s/%s: %s", key, circuit, case_id, exc)
                    continue

                if key == "hearings":
                    case["hearings"] = parse_hearings(tab_html)
                elif key == "advocates":
                    case["advocates"] = parse_advocates(tab_html)
                elif key == "parties":
                    case["parties"] = parse_parties(tab_html)
                elif key == "applications":
                    case["applications"] = parse_applications(tab_html)
                elif key == "connected":
                    case["connected_cases"] = parse_connected(tab_html)
                elif key == "govt_depts":
                    case["govt_depts"] = parse_govt_depts(tab_html)
                elif key == "discharge":
                    case["discharge_history"] = parse_simple_history(tab_html)
                elif key == "aros":
                    case["aros_history"] = parse_simple_history(tab_html)

        label = (
            case.get("case_no_raw")
            or case.get("case_title")
            or ("(orphan profile)" if case.get("profile_id_missing") else "(untitled)")
        )
        log.info(
            "OK %s/%s | %s | hearings=%s advocates=%s%s",
            circuit,
            case_id,
            label,
            len(case.get("hearings") or []),
            len(case.get("advocates") or []),
            " [incomplete profile]" if case.get("profile_id_missing") else "",
        )
        return case

    def scrape_ids(self, circuit: str, case_ids: Iterable[int]) -> int:
        saved = 0
        for case_id in case_ids:
            if self.already_done(circuit, case_id):
                log.info("Skip already-done %s/%s", circuit, case_id)
                continue
            try:
                case = self.fetch_case(circuit, case_id)
                if case is None:
                    self.mark_done(circuit, case_id, missing=True)
                    continue
                self.append_case(case)
                self.mark_done(circuit, case_id, missing=False)
                saved += 1
            except Exception as exc:  # noqa: BLE001 - keep run alive
                log.exception("Failed %s/%s: %s", circuit, case_id, exc)
                self.mark_error(circuit, case_id, str(exc))
        return saved

    def export(self) -> tuple[Path, Path]:
        if not self.jsonl_path.exists():
            raise FileNotFoundError(f"No data at {self.jsonl_path}")

        latest: dict[tuple[str, int], dict[str, Any]] = {}
        with self.jsonl_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                key = (str(obj.get("circuit")), int(obj.get("case_id")))
                latest[key] = obj  # last line wins
        cases = list(latest.values())

        json_path = self.data_dir / "cases.json"
        json_path.write_text(json.dumps(cases, ensure_ascii=False, indent=2), encoding="utf-8")

        case_rows = []
        hearing_rows = []
        advocate_rows = []
        nested_keys = {
            "hearings",
            "advocates",
            "parties",
            "applications",
            "connected_cases",
            "govt_depts",
            "discharge_history",
            "aros_history",
        }
        for case in cases:
            flat = {k: v for k, v in case.items() if k not in nested_keys}
            case_rows.append(flat)
            for h in case.get("hearings") or []:
                hearing_rows.append(
                    {
                        "circuit": case["circuit"],
                        "case_id": case["case_id"],
                        **h,
                    }
                )
            for a in case.get("advocates") or []:
                advocate_rows.append(
                    {
                        "circuit": case["circuit"],
                        "case_id": case["case_id"],
                        **a,
                    }
                )

        xlsx_path = self.data_dir / "cases.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            pd.DataFrame(case_rows).to_excel(writer, sheet_name="cases", index=False)
            pd.DataFrame(hearing_rows).to_excel(writer, sheet_name="hearings", index=False)
            pd.DataFrame(advocate_rows).to_excel(writer, sheet_name="advocates", index=False)

        log.info("Exported %s cases -> %s and %s", len(cases), json_path, xlsx_path)
        return json_path, xlsx_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polite SHC case scraper with checkpoints")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--circuits", nargs="+", default=["khi"], choices=list(CIRCUITS))
    p.add_argument("--start-id", type=int, default=None, help="Inclusive start case id")
    p.add_argument("--end-id", type=int, default=None, help="Inclusive end case id")
    p.add_argument(
        "--ids",
        type=int,
        nargs="+",
        default=None,
        help="Explicit case ids (overrides start/end)",
    )
    p.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Sample mode: scrape N known working Karachi ids only (safe test)",
    )
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Base delay between requests")
    p.add_argument("--jitter", type=float, default=DEFAULT_JITTER)
    p.add_argument("--profile-only", action="store_true", help="Skip tab endpoints")
    p.add_argument("--export-only", action="store_true", help="Rebuild JSON/Excel from jsonl")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


SAMPLE_IDS = [372276, 218546, 463057]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    global log
    log = setup_logging(args.data_dir / "logs", verbose=args.verbose)
    install_dns_failover()

    scraper = SHCScraper(
        data_dir=args.data_dir,
        delay=args.delay,
        jitter=args.jitter,
        fetch_tabs=not args.profile_only,
    )

    if args.export_only:
        scraper.export()
        return 0

    if args.sample is not None:
        ids = SAMPLE_IDS[: max(1, args.sample)]
        circuits = ["khi"]
        log.info("SAMPLE mode: circuits=%s ids=%s delay=%.1fs", circuits, ids, args.delay)
    else:
        circuits = args.circuits
        if args.ids:
            ids = args.ids
        elif args.start_id is not None and args.end_id is not None:
            if args.end_id < args.start_id:
                log.error("--end-id must be >= --start-id")
                return 2
            ids = list(range(args.start_id, args.end_id + 1))
        else:
            log.error("Provide --sample N, or --ids ..., or --start-id/--end-id")
            return 2

    total_saved = 0
    started = time.monotonic()
    for circuit in circuits:
        log.info("=== Circuit %s (%s) | %s ids ===", circuit, CIRCUITS[circuit], len(ids))
        total_saved += scraper.scrape_ids(circuit, ids)

    json_path, xlsx_path = scraper.export()
    elapsed = time.monotonic() - started
    log.info(
        "Done. saved=%s elapsed=%.1fs json=%s excel=%s",
        total_saved,
        elapsed,
        json_path,
        xlsx_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
