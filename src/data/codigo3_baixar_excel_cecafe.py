# -*- coding: utf-8 -*-
# Código 3: Baixar Arquivo Excel (equivalente em Python)

import requests
from datetime import date
from pathlib import Path

url = "https://www.cecafe.com.br/site/wp-content/uploads/graficos/cecafe-exportacao-resumo-diario.xlsx"

resp = requests.get(url, timeout=720)
if resp.status_code == 200:
    folder = Path("data/raw/cecafe")
    folder.mkdir(parents=True, exist_ok=True)  # Garante que a pasta existe
    filename = folder / f"{date.today().strftime('%d-%m-%Y')}.xlsx"
    filename.write_bytes(resp.content)
    print(f"Arquivo salvo como {filename}")
else:
    print("Falha ao baixar o arquivo")
