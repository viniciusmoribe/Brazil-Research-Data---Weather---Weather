# -*- coding: utf-8 -*-
# Código 6: Web Scraping de Indicadores de Café + NOAA VHI + USD/BRL (equivalente em Python)

import io
import re
import requests
import pandas as pd
from pathlib import Path
from datetime import date
import yfinance as yf

# -------- CEPEA --------
url_cepea = "https://www.cepea.esalq.usp.br/br/indicador/cafe.aspx"
tables = pd.read_html(url_cepea, flavor="lxml")

cafe_arabica = tables[0].copy()
cafe_robusta = tables[1].copy()


def convert_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def br_to_float(s):
        s = str(s)
        s = s.replace(".", "").replace("%", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return pd.NA

    if "Valor R$" in df.columns:
        df["Valor R$"] = df["Valor R$"].map(br_to_float)
    if "Valor US$" in df.columns:
        df["Valor US$"] = df["Valor US$"].map(br_to_float)
    if "Var./Dia" in df.columns:
        df["Var./Dia"] = df["Var./Dia"].map(br_to_float) / 100.0
    if "Var./Mês" in df.columns:
        df["Var./Mês"] = df["Var./Mês"].map(br_to_float) / 100.0
    return df


cafe_arabica = convert_columns(cafe_arabica)
cafe_robusta = convert_columns(cafe_robusta)

output_dir = Path(
    r"C:/Users/vinicius.pereira/OneDrive - ED & F Man Holding Limited/Brazil Diffs - Documents/BaseCepea"
)
output_dir.mkdir(parents=True, exist_ok=True)

output_file_arabica = output_dir / f"Arabica_{date.today().isoformat()}.csv"
output_file_robusta = output_dir / f"Conilon_{date.today().isoformat()}.csv"

cafe_arabica.to_csv(output_file_arabica, index=False, encoding="utf-8")
cafe_robusta.to_csv(output_file_robusta, index=False, encoding="utf-8")
print("Dados do CEPEA foram salvos em arquivos CSV com sucesso!")

# -------- NOAA VHI --------
url_noaa = (
    "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin.php"
    "?provinceID=13&country=BRA&yearlyTag=Weekly&type=Parea_VHI&TagCropland=ACOF&year1=1982&year2=2024"
)

resp = requests.get(url_noaa, timeout=120)
if resp.status_code == 200:
    text = resp.text
    lines = text.splitlines()
    # encontra primeira linha de dados que começa com ano (ex: 1982)
    start_idx = next(
        (i for i, ln in enumerate(lines) if re.match(r"^\s*1982", ln)), None
    )
    if start_idx is not None:
        data_lines = lines[start_idx:]
        data_csv = "\n".join(data_lines)
        df_noaa = pd.read_csv(io.StringIO(data_csv), header=None)
        print(df_noaa.head())
    else:
        print("Dados NOAA: início não encontrado.")
else:
    print(f"Erro NOAA: Status {resp.status_code}")

# -------- USD/BRL Yahoo Finance --------
symbol = "BRL=X"  # USD/BRL
start_date = "2020-01-01"
end_date = None  # até hoje

hist = yf.download(symbol, start=start_date, end=end_date, progress=False)
if hist is not None and not hist.empty:
    hist = hist.reset_index().rename(columns=str)
    hist.columns = ["date"] + [c for c in hist.columns if c != "date"]

    caminho = Path(
        r"C:/Users/vinicius.pereira/OneDrive - ED & F Man Holding Limited/Brazil Diffs - Documents/cotacoes_dolar.csv"
    )
    hist.to_csv(caminho, index=False, encoding="utf-8")
    print(f"Arquivo 'cotacoes_dolar.csv' salvo com sucesso em: {caminho}")
else:
    print("Erro: Não foi possível baixar dados USD/BRL do Yahoo Finance.")
