# -*- coding: utf-8 -*-
# Código 7 (revisado): Scraper XL Trend com sessão fortalecida, warm-up de cookies, CSRF automático e retries

import re
import json
import time
import random
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
INPUT_XLSX = "data/raw/Brazil.xlsx"
OUTPUT_XLSX = "data/processed/Rain_forecast_Brazil.xlsx"
TZ_LOCAL = ZoneInfo("America/Sao_Paulo")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
BASE_POST = "https://meteologix.com/br/ajax_pub/fcxlc?"

HEADERS_GET = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

HEADERS_POST_BASE = {
    "User-Agent": UA,
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://meteologix.com",
    "Connection": "keep-alive",
}

CONSENT_COOKIE = {
    # cookie de consentimento genérico; o site muitas vezes exige um valor presente
    "euconsent-v2": "CP-mp8AP-mp8AAGABCENAzEgAP_gAAAAABBoJphFBCpMDWFAMGBVAJAgSYAU19AQIAQAABCAAwAFAAGA4IAA0QAAEAQAAAAAAAAAgVABAAAAAABEAACAAAAEAQBEAAAAgAAIAAAAAAEQQgBAAAgAAAAAAAAIAAABAwQAkACAIQKEBEAghIAACAAAAIABAACAAAMACEAYAAAAAAIAAIBAAAIEEAIAAAEAAQAAAAAAAAAAAAAAAAAAgAAALCQIAAEAAVAA4ACAAGgARAAmABvAD8AISAQwBEgCOAEsAMOAfYB-gEUAI0AXMAvQBigDaAG4AUOAvMBhoDVwG5gOCAcmA8cCEIEOQgAQAGQB_QIGAQuHQHAAKgAcABAADQAIgATAA3gB-gEMARIAlgBhgDRgH2AfsBFAEWALmAYoA2gBuAEXgJkAUOAvMBhoDLAGrgOTAeOBDkcANAAQABcAGQAUABHAF6APkAf0BdADBAGmgNzAgYQgEgBMADeAI4AigBcwDFAG0AeOBCggACAAQAwQlAMAAQABwAIgATIBDAESAI4AfgBcwDFAIvAXmBCEkACAAuAywpAaAAqABwAEAANAAiABMACkAH6AQwBEgDRgH4AfoBFgC5gGKANoAbgBF4ChwF5gMNAZYA4IByYDxwIQgQ5KACgAFAAXABkAFsARwA-wF0AMEAbmBAwtADAEcAXoB44A.YAAAAAAAAAAA"
}

# --------------------------------------------------------------------
# Entrada
# --------------------------------------------------------------------
url_df = pd.read_excel(INPUT_XLSX)
url_df["URL"] = url_df["URL"].str.replace("temperature", "precipitation", regex=True)


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def parse_text_data(text_data):
    t = "".join(text_data)
    t = (
        t.replace("'", '"')
        .replace("\n", "")
        .replace(" ", "")
        .replace("[[", "[")
        .replace("]]", "]")
        .replace("data:", '"data":')
        .replace("name:", '"name":')
        .replace('long"name":', '"longname":')
        .replace("longname:", '"longname":')
        .replace("run:", '"run":')
        .replace("flag:", '"flag":')
        .replace("zIndex:", '"zIndex":')
        .replace("color:", '"color":')
        .replace("marker:", '"marker":')
        .replace("enabled:", '"enabled":')
        .replace('"data":[', '"data":[')
        .replace("],[", ",")
    )
    return t


def list_to_df(data_list, dtime_list, stn_names):
    out = []
    for data, dts in zip(data_list, dtime_list):
        df = pd.DataFrame([data], columns=dts)
        df.columns = [
            d.strftime("%d-%m-%Y")
            if not isinstance(d, str) and hasattr(d, "strftime")
            else d
            for d in df.columns
        ]
        out.append(df)
    if not out:
        return pd.DataFrame(columns=["Stn Name"])
    result = pd.concat(out, ignore_index=True)
    result.insert(0, "Stn Name", stn_names[: len(result)])
    return result


def extract_series(item):
    raw = item["data"]  # [t1, v1, t2, v2, ...] epoch ms
    ts_ms = raw[0::2]
    vals = raw[1::2]
    dts = [
        datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
        .astimezone(TZ_LOCAL)
        .replace(tzinfo=None)
        for ms in ts_ms
    ]
    return vals, dts


def get_csrf_token(html: str) -> str | None:
    m = re.search(
        r'name=["\']csrf-token["\']\s+content=["\'](.*?)["\']', html, flags=re.I
    )
    if not m:
        m = re.search(
            r'content=["\'](.*?)["\']\s+name=["\']csrf-token["\']', html, flags=re.I
        )
    return m.group(1) if m else None


def warm_up_session(sess: requests.Session):
    # seed cookies no domínio e consentimento
    sess.cookies.update(CONSENT_COOKIE)
    for url in ["https://meteologix.com/", "https://meteologix.com/br/"]:
        try:
            r = sess.get(url, headers=HEADERS_GET, timeout=20, allow_redirects=True)
            # não levantar exceção aqui; alguns retornam 403/redirects intermitentes
        except requests.RequestException:
            pass


def session_fetch(referer_url: str, city_id: str, max_retries: int = 5) -> str | None:
    s = requests.Session()
    warm_up_session(s)

    # Primeiro GET do referer (tentar cabeçalhos diferentes se 403)
    alt_sets = [
        HEADERS_GET,
        {**HEADERS_GET, "Referer": "https://meteologix.com/br/"},
        {"User-Agent": UA, "Accept": "*/*"},
    ]

    token = None
    for hs in alt_sets:
        try:
            r0 = s.get(referer_url, headers=hs, timeout=30, allow_redirects=True)
            if r0.status_code == 200:
                token = get_csrf_token(r0.text)
                break
        except requests.RequestException:
            time.sleep(0.6)

    headers_post = {**HEADERS_POST_BASE, "Referer": referer_url.strip()}
    if token:
        headers_post["X-CSRF-Token"] = token

    data = {"city_id": city_id, "lang": "en", "unit_t": "celsius"}

    for attempt in range(max_retries):
        r = s.post(BASE_POST, headers=headers_post, data=data, timeout=60)
        if r.status_code == 200 and "hc_data_rain_day" in r.text:
            return r.text
        if r.status_code in (403, 429, 500, 502, 503, 504):
            # tentar re-obter token e variar cabeçalhos
            try:
                r0 = s.get(
                    referer_url,
                    headers=random.choice(alt_sets),
                    timeout=25,
                    allow_redirects=True,
                )
                token2 = get_csrf_token(r0.text)
                if token2:
                    headers_post["X-CSRF-Token"] = token2
            except requests.RequestException:
                pass
            time.sleep(1.2 * (attempt + 1) + random.random())
            continue
        # outros status: tenta mais uma vez com Accept simples
        headers_post["Accept"] = "*/*"
        time.sleep(0.8)
    return None


# --------------------------------------------------------------------
# Coletores por modelo
# --------------------------------------------------------------------
ECMWF6z_18z, ECMWF6z_18z_dt = [], []
ECMWF_0_12, ECMWF_0_12_dt = [], []
GFS, GFS_dt = [], []
GEM, GEM_dt = [], []
access_g, access_g_dt = [], []
Icon, Icon_dt = [], []
Norway_ecmwf, Norway_ecmwf_dt = [], []
Ukmo, Ukmo_dt = [], []
MULTI_GLOBAL, MULTI_GLOBAL_dt = [], []

MODEL_MAP = {
    "ECMWF(0/6/12/18)": (ECMWF6z_18z, ECMWF6z_18z_dt),
    "ECMWF(0/12)": (ECMWF_0_12, ECMWF_0_12_dt),
    "GFS": (GFS, GFS_dt),
    "GEM": (GEM, GEM_dt),
    "ACCESS-G": (access_g, access_g_dt),
    "ICON": (Icon, Icon_dt),
    "Norway-ECMWF": (Norway_ecmwf, Norway_ecmwf_dt),
    "UKMO": (Ukmo, Ukmo_dt),
    "MULTI-GLOBAL": (MULTI_GLOBAL, MULTI_GLOBAL_dt),
}

# --------------------------------------------------------------------
# Scrape
# --------------------------------------------------------------------
stn_names_collected = []

for _, row in url_df.iterrows():
    refererurl = str(row["URL"])
    stnid = refererurl[35:42]
    stn_names_collected.append(row["Stn Name"])

    html = session_fetch(refererurl, stnid)
    if not html:
        continue

    lines = html.splitlines()
    start_idx = next(
        (i + 1 for i, ln in enumerate(lines) if "hc_data_rain_day" in ln), 0
    )
    finish_idx = next(
        (
            i - 3
            for i, ln in enumerate(lines)
            if '<p class="graph-headline">Precipitation</p>' in ln
        ),
        len(lines),
    )
    text = parse_text_data(lines[start_idx:finish_idx])

    try:
        payload = json.loads(f"[{text}]")
    except json.JSONDecodeError:
        continue

    for item in payload:
        name = item.get("name")
        if name in MODEL_MAP:
            vals, dts = extract_series(item)
            MODEL_MAP[name][0].append(vals)
            MODEL_MAP[name][1].append(dts)

# --------------------------------------------------------------------
# DataFrames
# --------------------------------------------------------------------
ECMWF6z_18z_df = list_to_df(ECMWF6z_18z, ECMWF6z_18z_dt, stn_names_collected)
ECMWF_0_12_df = list_to_df(ECMWF_0_12, ECMWF_0_12_dt, stn_names_collected)
GFS_df = list_to_df(GFS, GFS_dt, stn_names_collected)
GEM_df = list_to_df(GEM, GEM_dt, stn_names_collected)
access_g_df = list_to_df(access_g, access_g_dt, stn_names_collected)
Icon_df = list_to_df(Icon, Icon_dt, stn_names_collected)
Norway_ecmwf_df = list_to_df(Norway_ecmwf, Norway_ecmwf_dt, stn_names_collected)
Ukmo_df = list_to_df(Ukmo, Ukmo_dt, stn_names_collected)
MULTI_GLOBAL_df = list_to_df(MULTI_GLOBAL, MULTI_GLOBAL_dt, stn_names_collected)

# --------------------------------------------------------------------
# Export
# --------------------------------------------------------------------
Path(Path(OUTPUT_XLSX).parent).mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUTPUT_XLSX) as writer:
    ECMWF6z_18z_df.to_excel(writer, sheet_name="ECMWF6z_18z", index=False)
    ECMWF_0_12_df.to_excel(writer, sheet_name="ECMWF_0_12", index=False)
    GFS_df.to_excel(writer, sheet_name="GFS", index=False)
    GEM_df.to_excel(writer, sheet_name="GEM", index=False)
    access_g_df.to_excel(writer, sheet_name="Access-G", index=False)
    Icon_df.to_excel(writer, sheet_name="Icon", index=False)
    Norway_ecmwf_df.to_excel(writer, sheet_name="Norway-ECMWF", index=False)
    Ukmo_df.to_excel(writer, sheet_name="UKMO", index=False)
    MULTI_GLOBAL_df.to_excel(writer, sheet_name="MULTI-GLOBAL", index=False)
