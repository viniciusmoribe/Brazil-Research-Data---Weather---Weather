# -*- coding: utf-8 -*-
# Código 5: Scraping de Dados de Vento (equivalente em Python)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path

save_directory = Path(
    r"C:/Users/vinicius.pereira/OneDrive - ED & F Man Holding Limited/Clima/Daily/Winds"
)
save_directory.mkdir(parents=True, exist_ok=True)


def scrape_and_save(url: str, output_file: str):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    node = soup.select_one(".history-table.desktop-table")
    if node is None:
        raise ValueError("Tabela não encontrada na página.")
    tables = pd.read_html(str(node))
    if not tables:
        raise ValueError("Falha ao ler tabela.")
    df = tables[0]
    if isinstance(df, pd.Series):
        df = df.map(
            lambda x: str(x).replace("Â", "").replace("Ã", "")
            if isinstance(x, str)
            else x
        )
    else:
        df = df.apply(
            lambda x: str(x).replace("Â", "").replace("Ã", "")
            if isinstance(x, str)
            else x
        )
    output_path = save_directory / output_file
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Salvo: {output_path}")


urls = [
    "https://www.wunderground.com/dashboard/pws/ISOMAT4/table/2024-08-31/2024-08-31/monthly",
    "https://www.wunderground.com/dashboard/pws/IRIOBA4/table/2024-08-31/2024-08-31/monthly",
    "https://www.wunderground.com/dashboard/pws/ILINHA1/table/2024-08-31/2024-08-31/monthly",
    "https://www.wunderground.com/dashboard/pws/IARACR4/table/2024-08-31/2024-08-31/monthly",
    "https://www.wunderground.com/dashboard/pws/ISANTO362/table/2024-08-31/2024-08-31/monthly",
]

output_files = [
    "ISOMAT4_Aug_2024.csv",
    "IRIOBA4_Aug_2024.csv",
    "ILINHA1_Aug_2024.csv",
    "IARACR4_Aug_2024.csv",
    "ISANTO362_Aug_2024.csv",
]

for u, f in zip(urls, output_files):
    scrape_and_save(u, f)

print("Dados foram salvos em arquivos CSV com sucesso!")
