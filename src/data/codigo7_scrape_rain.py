# -*- coding: utf-8 -*-
# Código 7: Scraper de precipitação XL Trend sem avisos de depreciação
# Ref: uso recomendado -> datetime.fromtimestamp(..., tz=datetime.timezone.utc)
# https://docs.python.org/3/library/datetime.html#datetime.datetime.utcfromtimestamp

import json
import pandas as pd
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# -----------------------------------------------------------------------------
# Entrada
# -----------------------------------------------------------------------------
url_df = pd.read_excel("data/raw/Brazil.xlsx")
url_df["URL"] = url_df["URL"].str.replace("temperature", "precipitation", regex=True)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
TZ_LOCAL = ZoneInfo("America/Sao_Paulo")  # UTC-3 com DST histórico correto


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


def list_to_df(data_list, dtime_list):
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
    result.insert(0, "Stn Name", list(url_df["Stn Name"].values[: len(result)]))
    return result


def extract_series(item):
    """Converte pares [epoch_ms, valor] para (datas_local, valores)."""
    raw = item["data"]
    # épocas estão intercaladas: [t1, v1, t2, v2, ...]
    ts_ms = raw[0::2]
    vals = raw[1::2]
    dts = [
        datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
        .astimezone(TZ_LOCAL)
        .replace(tzinfo=None)
        for ms in ts_ms
    ]
    return vals, dts


# -----------------------------------------------------------------------------
# Coletores por modelo
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Scrape
# -----------------------------------------------------------------------------
for url in url_df["URL"]:
    stnid = url[35:42]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://meteologix.com",
        "Referer": url.strip(),
    }
    data = {
        "city_id": stnid,
        "lang": "en",
        "unit_t": "celsius",
    }

    r = requests.post(
        "https://meteologix.com/br/ajax_pub/fcxlc?",
        headers=headers,
        data=data,
        timeout=90,
    )
    r.raise_for_status()
    lines = r.text.splitlines()

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

# -----------------------------------------------------------------------------
# DataFrames
# -----------------------------------------------------------------------------
ECMWF6z_18z_df = list_to_df(ECMWF6z_18z, ECMWF6z_18z_dt)
ECMWF_0_12_df = list_to_df(ECMWF_0_12, ECMWF_0_12_dt)
GFS_df = list_to_df(GFS, GFS_dt)
GEM_df = list_to_df(GEM, GEM_dt)
access_g_df = list_to_df(access_g, access_g_dt)
Icon_df = list_to_df(Icon, Icon_dt)
Norway_ecmwf_df = list_to_df(Norway_ecmwf, Norway_ecmwf_dt)
Ukmo_df = list_to_df(Ukmo, Ukmo_dt)
MULTI_GLOBAL_df = list_to_df(MULTI_GLOBAL, MULTI_GLOBAL_dt)

# -----------------------------------------------------------------------------
# Exporta
# -----------------------------------------------------------------------------
with pd.ExcelWriter("data/processed/Rain_forecast_Brazil.xlsx") as writer:
    ECMWF6z_18z_df.to_excel(writer, sheet_name="ECMWF6z_18z", index=False)
    ECMWF_0_12_df.to_excel(writer, sheet_name="ECMWF_0_12", index=False)
    GFS_df.to_excel(writer, sheet_name="GFS", index=False)
    GEM_df.to_excel(writer, sheet_name="GEM", index=False)
    access_g_df.to_excel(writer, sheet_name="Access-G", index=False)
    Icon_df.to_excel(writer, sheet_name="Icon", index=False)
    Norway_ecmwf_df.to_excel(writer, sheet_name="Norway-ECMWF", index=False)
    Ukmo_df.to_excel(writer, sheet_name="UKMO", index=False)
    MULTI_GLOBAL_df.to_excel(writer, sheet_name="MULTI-GLOBAL", index=False)
