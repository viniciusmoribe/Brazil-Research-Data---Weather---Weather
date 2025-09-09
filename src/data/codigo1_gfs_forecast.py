# -*- coding: utf-8 -*-
# Código 1: Processamento de Previsões GFS (equivalente em Python)

import re
import requests
from pathlib import Path
from datetime import date

BASE_DIR = Path(
    r"C:/Users/vinicius.pereira/OneDrive - ED & F Man Holding Limited/Clima/Daily/GFS/Forecast"
)
BASE_DIR.mkdir(parents=True, exist_ok=True)

current_date = date.today().strftime("%d-%m-%Y")
file_name = BASE_DIR / f"{current_date}.txt"

url_list_path = BASE_DIR / "gfsupdatedurllist.txt"
response_tmp = BASE_DIR / "response.txt"

cookies_string = "_live_xx_csrf_image=2febdbe850d0374afcf528ffe33bbd967bbcca66ccfac690617d69c4dcb36257a%3A2%3A%7Bi%3A0%3Bs%3A19%3A%22_live_xx_csrf_image%22%3Bi%3A1%3Bs%3A40%3A%22a12a5246199b2e145d3fb9a926f18079bb0c314c%22%3B%7D; _csrf=0ff96b65d424b84f36386a8dd0508ab6d62d0e84cc823d74bc53eac96c1b6a77a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22qzRzxHyCnACgJVifjCI-wQguhcGaub44%22%3B%7D; _sp_v1_uid=1:692:5ed2d058-66ea-4be9-bb4e-1549d575c732; _sp_v1_data=2:378381:1631953984:0:6:0:6:0:0:_:-1; _sp_v1_ss=1:H4sIAAAAAAAAAItWqo5RKimOUbKKBjLyQAyD2lidGKVUEDOvNCcHyC4BK6iurVXSQVFegqQGq3oqGRSjlJmXWQJkGZoZG1qaGltaWhBnNj63EassFgA5qKJCIwEAAA%3D%3D; _sp_v1_opt=1:login|true:last_id|11:; _sp_v1_consent=1!1:1:1:0:0:0; _sp_v1_csv=null; _sp_v1_lt=1:; consentUUID=951e2981-1fe8-4eea-81ed-ed795fdc0c58; axd=4272046225751507569; session_id=45708977-047c-43a9-bd78-366e6a75c160; live_xx_history=6a208e388855c7fa39bad20eae82ea9c625df7377ebfb10de83222b1d61087aca%3A2%3A%7Bi%3A0%3Bs%3A15%3A%22live_xx_history%22%3Bi%3A1%3Ba%3A1%3A%7Bi%3A0%3Bi%3A3472603%3B%7D%7D"

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "X-CSRF-Token": "2HQASjoZ-7XwwRySosuCr5FDOyXs8sX-HiAORlAm3aKpDlIwQlGC9p6AX_XonevJ-wByCJujoot2Q0knJUTplg==",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://meteologix.com",
    "DNT": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "TE": "trailers",
    "Cookie": cookies_string,
}


def parse_slice(lines, start_1based, end_1based):
    # R usa 1-based e inclusivo; Python 0-based e fim exclusivo
    return lines[start_1based - 1 : end_1based]


def extract_values(block_lines):
    out = []
    for j in block_lines:
        m = re.search(r",(.+?)\]", j)
        if m:
            try:
                out.append(round(float(m.group(1)), 1))
            except ValueError:
                pass
    return out


def main():
    if not url_list_path.exists():
        raise FileNotFoundError(str(url_list_path))

    with url_list_path.open("r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    with file_name.open("a", encoding="utf-8") as out:
        for i in lines:
            refererurl = i
            stnname = i[43 : len(i) - 37].title()
            stnid = i[35:42]

            headers = dict(COMMON_HEADERS)
            headers["Referer"] = refererurl

            data = {
                "city_id": stnid,
                "model": "usa",
                "model_view": "range",
                "param": "niederschlag24h",
            }

            resp = requests.post(
                "https://meteologix.com/in/ajax/ensemble",
                headers=headers,
                data=data,
                timeout=120,
            )
            resp.raise_for_status()
            response_tmp.write_text(resp.text, encoding="utf-8")

            fp = response_tmp.read_text(encoding="utf-8").splitlines()
            x = parse_slice(fp, 21, 36)
            chartdata = [stnname, *extract_values(x)]
            print(chartdata)
            out.write(" ".join(map(str, chartdata)) + "\n")


if __name__ == "__main__":
    main()
