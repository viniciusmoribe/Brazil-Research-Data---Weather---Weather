# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 12:48:40 2024

@author: mahendraprasad.mali, updated for rain by vinicius.pereira
"""

# =============================================================================
# Required Libraries
# =============================================================================

# from urllib.request import Request, urlopen
import pandas as pd
import json
import requests
from datetime import datetime
import pytz

# Lê o arquivo de municípios
url_df = pd.read_excel("data/raw/Brazil Tracking Municipalities.xlsx")
url_df["URL"] = url_df["URL"].str.replace("temperature", "precipitation", regex=True)


# Função para baixar e processar dados de chuva sem arquivos temporários
def baixar_e_processar(url, station):
    response = requests.get(url)
    lines = response.text.splitlines()
    # Processamento dos dados (exemplo)
    dados = [line.strip().split(";") for line in lines if line.strip()]
    return dados


# Exemplo de uso
for idx, row in url_df.iterrows():
    url = row["URL"]
    station = row["Stn Name"]
    dados = baixar_e_processar(url, station)
    # ... (restante do processamento)


# =============================================================================
# Some helper functions
# =============================================================================


def parse_text_data(text_data):
    text = "".join(text_data)
    text = text.replace(
        "'", '"'
    )  # Replace single quotes with double quotes for JSON compatibility
    text = text.replace("\n", "")  # Remove newline characters
    text = text.replace(" ", "")  # Remove extra spaces
    return text


def list_to_df(data_list, dtime_list):  # , at_hour):
    result_df = pd.DataFrame()

    for i in range(len(data_list)):
        temp_df = pd.DataFrame(data=data_list[i]).T
        temp_df.columns = dtime_list[i]
        # Convert columns to datetime if they are not already
        temp_df.columns = pd.to_datetime(temp_df.columns, errors="coerce")
        # Filter columns where conversion succeeded and hour matches
        # filtered_cols = [
        #    dtime
        #    for dtime in temp_df.columns
        #    if pd.notnull(dtime)
        #    and isinstance(dtime, datetime.datetime)
        #    and dtime.hour == at_hour
        # ]
        # temp_df = temp_df[filtered_cols]
        temp_df.columns = [
            col.strftime("%d-%m-%Y")
            if not isinstance(col, str) and hasattr(col, "strftime")
            else col
            for col in temp_df.columns
        ]
        result_df = pd.concat([result_df, temp_df])
        result_df.reset_index(drop=True, inplace=True)
    result_df.insert(loc=0, column="Stn Name", value=url_df["Stn Name"])
    result_df.reset_index(drop=True, inplace=True)
    return result_df


# =============================================================================
# Code to scrape XL Trend data
# =============================================================================

ECMWF6z_18z = []
ECMWF6z_18z_dt = []

ECMWF_0_12 = []
ECMWF_0_12_dt = []

GFS = []
GFS_dt = []

GEM = []
GEM_dt = []

access_g = []
access_g_dt = []

Icon = []
Icon_dt = []

Norway_ecmwf = []
Norway_ecmwf_dt = []

Ukmo = []
Ukmo_dt = []

MULTI_GLOBAL = []
MULTI_GLOBAL_dt = []

for url in url_df["URL"]:
    refererurl = url

    print(refererurl)

    # stnname = url[43:-25]
    stnid = url[35:42]

    # stnname = i[43:-38].capitalize()
    # stnid = i[35:42]

    cookies = {
        "_live_xx_csrf_image": "46c771a542b7fe048f1dabec94edfa2f1dd23b17fe268143baa983d43f9e7fc2a%3A2%3A%7Bi%3A0%3Bs%3A19%3A%22_live_xx_csrf_image%22%3Bi%3A1%3Bs%3A40%3A%226739d406648f6a754e36773c6fb4742c4e136f53%22%3B%7D",
        "_csrf": "8f07755e2989453b41c7a4ea973154584a2e74e73bd02b98ef88900a850287dfa%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22dhZrbcTS0_TcAfr05qGU6AYt2kED0D_Y%22%3B%7D",
        "euconsent-v2": "CP-mp8AP-mp8AAGABCENAzEgAP_gAAAAABBoJphFBCpMDWFAMGBVAJAgSYAU19AQIAQAABCAAwAFAAGA4IAA0QAAEAQAAAAAAAAAgVABAAAAAABEAACAAAAEAQBEAAAAgAAIAAAAAAEQQgBAAAgAAAAAAAAIAAABAwQAkACAIQKEBEAghIAACAAAAIABAACAAAMACEAYAAAAAAIAAIBAAAIEEAIAAAEAAQAAAAAAAAAAAAAAAAAAgAAALCQIAAEAAVAA4ACAAGgARAAmABvAD8AISAQwBEgCOAEsAMOAfYB-gEUAI0AXMAvQBigDaAG4AUOAvMBhoDVwG5gOCAcmA8cCEIEOQgAQAGQB_QIGAQuHQHAAKgAcABAADQAIgATAA3gB-gEMARIAlgBhgDRgH2AfsBFAEWALmAYoA2gBuAEXgJkAUOAvMBhoDLAGrgOTAeOBDkcANAAQABcAGQAUABHAF6APkAf0BdADBAGmgNzAgYQgEgBMADeAI4AigBcwDFAG0AeOBCggACAAQAwQlAMAAQABwAIgATIBDAESAI4AfgBcwDFAIvAXmBCEkACAAuAywpAaAAqABwAEAANAAiABMACkAH6AQwBEgDRgH4AfoBFgC5gGKANoAbgBF4ChwF5gMNAZYA4IByYDxwIQgQ5KACgAFAAXABkAFsARwA-wF0AMEAbmBAwtADAEcAXoB44A.YAAAAAAAAAAA",
        "_ga": "GA1.1.770210186.1715668298",
        "panoramaId_expiry": "1716878709857",
        "tis": "EP280%3A3914%7CEP286%3A3914",
        "cto_bidid": "SmtZx183cW1DJTJGd3BLcTRwMmViOWlWNWprejVLcHh5UThQRmRyam5BJTJGYldZcjBXNGZGODBOVVRaZXJuY2ZhYVRJUXpBYkJ2WmpOcXAwdzJRZG1DRGhCMEJNTlFHeU5OcjBIRDgxM0tNeHJlbTBqJTJGRyUyQjBwZldiMnZOZjMzMGpNUlVybyUyQjk",
        "__gads_ID": "78589d68ffbbfa35:T=1715668298:RT=1716793339:S=ALNI_MbNOai5XV-cZUKvbvX3boL05e2B_g",
        "__gpi_UID": "00000e1bdfc7c39e:T=1715668298:RT=1716793339:S=ALNI_MbPy7zkm8FTzbnzoJHO2wWjpVKXNA",
        "__eoi_ID": "ed99f42150fd0a3e:T=1715668298:RT=1716793339:S=AA-Afjbp6GnBLwcMJguDDWMq5Amy",
        "cto_bundle": "mkdpp18wMSUyRnZGYmE0MTBQSjVOS3VYZ0tMSmhaelBTMjcwY1NwOWcxejU1UWJyNUJyUHROdXJrcnFWa2twWE9HNzYlMkJrTSUyQmVWRVAwc1NUSjROdnd0dGhzbFpCZTNXMndvbVlzVXJDdlROMFlmVXRIdm5PcU1zY1FWcHA4Y3hOQ2tId1hNMVJrVXZhY3hkREFvUFN5MUhBYnBwVWclM0QlM0Q",
        "ga_X9W1CPMBES": "GS1.1.1716795328.25.0.1716795328.0.0.0",
        #'_sp_v1_uid': '1:692:5ed2d058-66ea-4be9-bb4e-1549d575c732',
        #'_sp_v1_data': '2:378381:1631953984:0:6:0:6:0:0:_:-1',
        #'_sp_v1_ss': '1:H4sIAAAAAAAAAItWqo5RKimOUbKKBjLyQAyD2lidGKVUEDOvNCcHyC4BK6iurVXSQVFegqQGq3oqGRSjlJmXWQJkGZoZG1qaGltaWhBnNj63EassFgA5qKJCIwEAAA%3D%3D',
        #'_sp_v1_opt': '1:login|true:last_id|11:',
        #'_sp_v1_consent': '1!1:1:1:0:0:0',
        #'_sp_v1_csv': 'null',
        #'_sp_v1_lt': '1:',
        "consentUUID": "5b2c6641-0c21-461c-803f-d7709311f409_31",
        "axd": "4361933779447805221",
        "session_id": "ab059f34-7fee-4f54-aafb-d968af860e6",
        "live_xx_history": "48e3ec113231616fa2ac2bfa8dad8683eb56013f11dda08b0b7a339d59161516a%3A2%3A%7Bi%3A0%3Bs%3A15%3A%22live_xx_history%22%3Bi%3A1%3Ba%3A2%3A%7Bi%3A0%3Bi%3A3472926%3Bi%3A1%3Bi%3A1586896%3B%7D%7D",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Pragma": "no-cache",
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "X-CSRF-Token": "TGp-_0Umklzt40ubrQp9vHvCd3K4Woj_3WPoEyUA6LEoAiSNJ0XGD928H_jsbA-MTrMwJ44b0YvvCK1XFUS36A==",
        #'Content-Type': 'text/html; charset=UTF-8',
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://meteologix.com",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": refererurl.strip(),
        #'Sec-Ch-Ua': '"Island";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        #'TE': 'trailers',
    }

    data = {
        "city_id": stnid,
        "lang": "en",
        "unit_t": "celsius",
        #'unit_v': 'kmh',
        #'unit_l': 'metrisch',
        #'unit_r': 'joule',
        #'unit_p': 'hpa',
        #'nf': 'point',
        #'tf': '0',
        #'func': 'xltrend',
        #'model': 'euro',
        #'model_view': 'range',
        #'param': 'niederschlag24h'
    }

    response = requests.post(
        "https://meteologix.com/br/ajax_pub/fcxlc?",
        headers=headers,
        cookies=cookies,
        data=data,
    )

    lines = response.text.splitlines()
    # Procura a linha que contém 'hc_data_rain_day'
    for idx, line in enumerate(lines):
        if "hc_data_rain_day" in line:
            start_idx = idx + 1
            break
    else:
        start_idx = 0  # fallback se não encontrar

    for idx, line in enumerate(lines):
        if '<p class="graph-headline">Precipitation</p>' in line:
            finish_idx = idx - 3
            break
    else:
        finish_idx = len(lines)  # fallback se não encontrar
    # Ajuste o range conforme necessário para capturar o bloco de dados
    text = lines[start_idx:finish_idx]

    text = parse_text_data(text)
    text = text.replace("[[", "[")
    text = text.replace("]]", "]")

    text = text.replace("'", '"')
    text = text.replace("data:", '"data":')
    text = text.replace("name:", '"name":')
    text = text.replace('long"name":', '"longname":')
    text = text.replace("longname:", '"longname":')
    text = text.replace("run:", '"run":')
    text = text.replace("flag:", '"flag":')
    text = text.replace("zIndex:", '"zIndex":')
    text = text.replace("color:", '"color":')
    text = text.replace("marker:", '"marker":')
    text = text.replace("enabled:", '"enabled":')
    text = text.replace('"data":[', '"data":[')

    text = text.replace("],[", ",")

    json_object = json.loads(f"[{text}]")

    for item in json_object:
        # print(item['name'])
        if item["name"] == "ECMWF(0/6/12/18)":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            ECMWF6z_18z.append(data)
            ECMWF6z_18z_dt.append(dt)

        elif item["name"] == "ECMWF(0/12)":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            ECMWF_0_12.append(data)
            ECMWF_0_12_dt.append(dt)

        elif item["name"] == "GFS":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            GFS.append(data)
            GFS_dt.append(dt)

        elif item["name"] == "GEM":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            GEM.append(data)
            GEM_dt.append(dt)

        elif item["name"] == "ACCESS-G":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            access_g.append(data)
            access_g_dt.append(dt)

        elif item["name"] == "ICON":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            Icon.append(data)
            Icon_dt.append(dt)

        elif item["name"] == "Norway-ECMWF":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            Norway_ecmwf.append(data)
            Norway_ecmwf_dt.append(dt)

        elif item["name"] == "UKMO":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            Ukmo.append(data)
            Ukmo_dt.append(dt)

        elif item["name"] == "MULTI-GLOBAL":
            data = item["data"]
            dt = [data[i] for i in range(len(data)) if i % 2 == 0]
            dt = [
                datetime.utcfromtimestamp(int(dtime / 1000))
                .replace(tzinfo=pytz.UTC)
                .astimezone(pytz.timezone("Etc/GMT+3"))
                .replace(tzinfo=None)
                for dtime in dt
            ]
            data = [data[i] for i in range(len(data)) if i % 2 != 0]
            MULTI_GLOBAL.append(data)
            MULTI_GLOBAL_dt.append(dt)

        else:
            print(f"New model detected, check - {refererurl}")


# =============================================================================
# Clean and create dataframe of scraped data
# =============================================================================

ECMWF6z_18z_df = list_to_df(ECMWF6z_18z, ECMWF6z_18z_dt)  # , at_hour=6)

ECMWF_0_12_df = list_to_df(ECMWF_0_12, ECMWF_0_12_dt)  # , at_hour=6)
GFS_df = list_to_df(GFS, GFS_dt)  # , at_hour=6)
GEM_df = list_to_df(GEM, GEM_dt)  # , at_hour=6)
access_g_df = list_to_df(access_g, access_g_dt)  # , at_hour=6)
Icon_df = list_to_df(Icon, Icon_dt)  # , at_hour=6)
Norway_ecmwf_df = list_to_df(Norway_ecmwf, Norway_ecmwf_dt)  # , at_hour=6)
Ukmo_df = list_to_df(Ukmo, Ukmo_dt)  # , at_hour=6)
MULTI_GLOBAL_df = list_to_df(MULTI_GLOBAL, MULTI_GLOBAL_dt)  # , at_hour=6)


# =============================================================================
# Export all data to excel sheets models wise
# =============================================================================


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
