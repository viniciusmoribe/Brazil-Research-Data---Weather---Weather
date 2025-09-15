# -*- coding: utf-8 -*-
# VHI downloader with robust week fallback per year (fixes Rondônia/2024 gaps)
import os
import time
import requests
from PIL import Image, UnidentifiedImageError
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+


# =========================
# Configurações
# =========================
def get_initial_week() -> int:
    env_week = os.getenv("VHI_WEEK", "").strip()
    if env_week.isdigit():
        w = int(env_week)
        if 1 <= w <= 53:
            print(f"[INFO] Semana inicial pelo ambiente VHI_WEEK={w}")
            return w
        else:
            print(f"[WARN] VHI_WEEK inválida ({env_week}), usando semana ISO atual.")
    iso_week = datetime.now(ZoneInfo("America/Sao_Paulo")).isocalendar().week
    print(f"[INFO] Semana ISO atual (America/Sao_Paulo): {iso_week}")
    return iso_week


def get_max_lookback() -> int:
    env = os.getenv("VHI_MAX_LOOKBACK", "").strip()
    return max(0, int(env)) if env.isdigit() else 10  # padrão


# Saída
output_dir = "vhi_images"
os.makedirs(output_dir, exist_ok=True)

# =========================
# URLs por região (placeholders: {year}, {week})
# =========================
regions = {
    "Minas_Gerais": (
        "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
        "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
        "&provinceID=13&latlonRange=-22.922747,-51.045883,-14.233427,-39.856762"
        "&title=VHI%20of%20current%20year&type=VHI&week={year},{week}"
    ),
    "Sao_Paulo": (
        "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
        "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
        "&provinceID=25&latlonRange=-25.303192,-53.109604,-19.779652,-43.859108"
        "&title=VHI%20of%20current%20year&type=VHI&week={year},{week}"
    ),
    "Espirito_Santo": (
        "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
        "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
        "&provinceID=8&latlonRange=-21.297190,-41.878914,-17.891941,-39.6"
        "&title=VHI%20of%20current%20year&type=VHI&week={year},{week}"
    ),
    "Rondonia": (
        "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
        "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
        "&provinceID=22&latlonRange=-13.557581,-66.806473,-7.969309,-59.774288"
        "&title=VHI%20of%20current%20year&type=VHI&week={year},{week}"
    ),
    "Bahia": (
        "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
        "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
        "&provinceID=5&latlonRange=-18.349859,-46.617046,-8.533636,-37.349030"
        "&title=VHI%20of%20current%20year&type=VHI&week={year},{week}"
    ),
}

# ordem: primeiro é o “ano mais recente” (aplica fallback de ANO só nele)
years = [2025, 2024, 2023, 2022]

# Cabeçalhos para evitar bloqueios ocasionais
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VHI-Downloader/1.0; +https://www.star.nesdis.noaa.gov/)",
    "Referer": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/vh_browseVH.php",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}


# =========================
# Utilidades de imagem
# =========================
def is_image_response(resp: requests.Response) -> bool:
    if resp.status_code != 200:
        return False
    ctype = resp.headers.get("Content-Type", "").lower()
    if "image" not in ctype:
        # Alguns retornos errôneos vêm como text/html
        return False
    try:
        Image.open(BytesIO(resp.content))
        return True
    except Exception:
        return False


def has_no_data_banner(img: Image.Image) -> bool:
    """Detecta o banner vermelho 'Sorry, data are not available!' (robusto a tamanhos)."""
    w, h = img.size
    # Examina 12% superiores e 12% inferiores (algumas figuras mudaram layout)
    bands = [
        img.crop((0, 0, w, max(1, int(h * 0.12)))).convert("RGB"),
        img.crop((0, max(0, int(h * 0.88)), w, h)).convert("RGB"),
    ]
    for band in bands:
        red_pixels = 0
        for r, g, b in band.getdata():
            # vermelho “banner” (tolerante)
            if r >= 180 and g <= 100 and b <= 100:
                red_pixels += 1
        ratio = red_pixels / max(1, band.size[0] * band.size[1])
        if ratio > 0.00025:
            return True
    return False


def combine_images_horizontally(images):
    max_h = max(i.height for i in images)
    total_w = sum(i.width for i in images)
    canvas = Image.new("RGB", (total_w, max_h), (255, 255, 255))
    x = 0
    for im in images:
        y = (max_h - im.height) // 2
        canvas.paste(im, (x, y))
        x += im.width
    return canvas


def combine_images_vertically(images):
    max_w = max(i.width for i in images)
    total_h = sum(i.height for i in images)
    canvas = Image.new("RGB", (max_w, total_h), (255, 255, 255))
    y = 0
    for im in images:
        x = (max_w - im.width) // 2
        canvas.paste(im, (x, y))
        y += im.height
    return canvas


# =========================
# Busca semana válida global (usa ano mais recente e Minas Gerais como referência)
# =========================
def find_available_week(start_week: int, max_lookback: int) -> int:
    region_ref = next(iter(regions))  # primeira região do dicionário
    year_ref = years[0]
    wk = start_week
    for _ in range(max_lookback + 1):
        url = regions[region_ref].format(year=year_ref, week=wk)
        print(f"[TESTE] Semana {wk} (ref: {region_ref} {year_ref})")
        try:
            r = requests.get(url, timeout=30, headers=DEFAULT_HEADERS)
            if is_image_response(r):
                img = Image.open(BytesIO(r.content))
                if not has_no_data_banner(img):
                    print(f"[OK] Semana válida: {wk}")
                    return wk
                else:
                    print(f"[INFO] Semana {wk} sem dados (banner).")
        except requests.RequestException as e:
            print(f"[WARN] Rede: {e}")
        wk -= 1
        if wk < 1:
            wk = 53
    raise RuntimeError("Sem semana válida dentro do lookback.")


# =========================
# Download com FALLBACK DE SEMANA por ano
#  - Para o ano mais recente: tenta (semana), se faltar dados, recua semanas;
#    se ainda assim não achar, cai para anos anteriores (ano fallback).
#  - Para anos anteriores: tenta (semana) e recua semanas dentro do MESMO ano.
# =========================
def download_image_with_fallback(
    region: str, base_year: int, start_week: int, max_lookback_weeks: int
):
    idx = years.index(base_year)
    year_candidates = (
        years[idx:] if idx == 0 else [base_year]
    )  # fallback de ANO só para o mais recente
    for yr in year_candidates:
        wk = start_week
        for step in range(max_lookback_weeks + 1):
            url = regions[region].format(year=yr, week=wk)
            print(f"Baixando {region} - ano {yr} - semana {wk} (tentativa {step + 1})")
            try:
                r = requests.get(url, timeout=60, headers=DEFAULT_HEADERS)
            except requests.RequestException as e:
                print(f"[ERRO] Rede: {e}")
                time.sleep(0.5)
                wk = wk - 1 if wk > 1 else 53
                continue

            if not is_image_response(r):
                print(
                    f"[ERRO] Resposta inválida (status {r.status_code}, tipo={r.headers.get('Content-Type')})"
                )
                time.sleep(0.25)
                wk = wk - 1 if wk > 1 else 53
                continue

            try:
                img = Image.open(BytesIO(r.content))
            except UnidentifiedImageError:
                print("[ERRO] Conteúdo não é imagem")
                time.sleep(0.25)
                wk = wk - 1 if wk > 1 else 53
                continue

            if has_no_data_banner(img):
                print(f"[INFO] {region} {yr} semana {wk}: sem dados (banner).")
                wk = wk - 1 if wk > 1 else 53
                continue

            # OK
            return img, yr, wk

        print(f"[WARN] Não achou semana válida em {yr} para {region}.")
        # se base_year era o mais recente, permite cair para próximo ano (ex.: 2025 -> 2024)

    print(
        f"[WARN] Falhou para {region} partindo de {base_year} (com fallback de semana{' e ano' if years.index(base_year) == 0 else ''})."
    )
    return None, None, None


# =========================
# Execução principal
# =========================
initial_week = get_initial_week()
max_lookback = get_max_lookback()
week = find_available_week(initial_week, max_lookback)
print(f"[INFO] Semana usada (referência): {week}")

# 1) Download por região/ano e combinação horizontal
saved_weeks = {}  # para log por região
for region_name in regions.keys():
    imgs, used_years = [], []
    used_weeks = []
    for i, year in enumerate(years):
        img, used_year, used_week = download_image_with_fallback(
            region_name, year, week, max_lookback_weeks=max_lookback
        )
        if img is None:
            print(f"[WARN] Ignorando {region_name} {year}")
            continue
        # nomes FIXOS (sem semana)
        path = os.path.join(output_dir, f"{region_name}_{used_year}.png")
        img.save(path)
        imgs.append(img)
        used_years.append(used_year)
        used_weeks.append(used_week)

    saved_weeks[region_name] = used_weeks

    if imgs:
        combo = combine_images_horizontally(
            list(reversed(imgs))
        )  # direita→esquerda anos crescentes
        out = os.path.join(output_dir, f"{region_name}_combined.png")
        combo.save(out)
        print(
            f"[OK] Combinada: {out} | anos usados (esq→dir): {list(reversed(used_years))} | semanas: {list(reversed(used_weeks))}"
        )
    else:
        print(f"[WARN] Sem imagens válidas para {region_name}")


# 2) Combinações verticais desejadas (nomes FIXOS)
def juntar_vertical(top_region, bottom_region, out_name):
    p1 = os.path.join(output_dir, f"{top_region}_combined.png")
    p2 = os.path.join(output_dir, f"{bottom_region}_combined.png")
    if os.path.exists(p1) and os.path.exists(p2):
        final = combine_images_vertically([Image.open(p1), Image.open(p2)])
        out = os.path.join(output_dir, out_name)
        final.save(out)
        print(f"[OK] Vertical: {out}")
    else:
        print(f"[ERRO] Arquivos não encontrados: {p1} | {p2}")


juntar_vertical("Minas_Gerais", "Sao_Paulo", "combined_Minas_Gerais_Sao_Paulo.png")
juntar_vertical("Rondonia", "Espirito_Santo", "combined_Rondonia_Espirito_Santo.png")

print(f"[DONE] Semana final de referência (log): {week}")
with open(os.path.join(output_dir, "week.txt"), "w") as f:
    f.write(str(week))
