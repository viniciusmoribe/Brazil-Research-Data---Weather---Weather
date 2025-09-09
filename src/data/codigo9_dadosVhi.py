import os
import requests
from PIL import Image, UnidentifiedImageError
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+


# =========================
# Configurações
# =========================
def get_initial_week() -> int:
    """Semana alvo: VHI_WEEK (1..53) ou ISO atual em America/Sao_Paulo."""
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
    """Máximo de semanas para voltar quando a semana atual não tiver dados."""
    env = os.getenv("VHI_MAX_LOOKBACK", "").strip()
    return max(0, int(env)) if env.isdigit() else 10  # padrão


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

# ordem: primeiro é o “ano mais recente” (aplica fallback de ano só nele)
years = [2025, 2024, 2023, 2022]


# =========================
# Utilidades de imagem
# =========================
def is_image_response(resp: requests.Response) -> bool:
    if resp.status_code != 200:
        return False
    try:
        Image.open(BytesIO(resp.content))
        return True
    except Exception:
        return False


def has_no_data_banner(img: Image.Image) -> bool:
    """Detecta o banner vermelho 'Sorry, data are not available!' no topo."""
    w, h = img.size
    top_band = img.crop((0, 0, w, int(h * 0.08))).convert("RGB")  # 8% superior
    red_pixels = sum(
        1 for r, g, b in top_band.getdata() if r >= 200 and g <= 80 and b <= 80
    )
    ratio = red_pixels / max(1, top_band.size[0] * top_band.size[1])
    return ratio > 0.0004  # limiar baixo; texto ocupa área pequena


def combine_images_horizontally(images):
    """Combina horizontalmente preservando escala (usa padding branco)."""
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
    """Combina verticalmente preservando escala (usa padding branco)."""
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
# Fallback de semana (usa ano mais recente como referência)
# =========================
def find_available_week(start_week: int, max_lookback: int) -> int:
    region_ref = next(iter(regions))
    year_ref = years[0]
    wk = start_week
    for _ in range(max_lookback + 1):
        url = regions[region_ref].format(year=year_ref, week=wk)
        print(f"[TESTE] Semana {wk} (ref: {region_ref} {year_ref})")
        try:
            r = requests.get(url, timeout=30)
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
# Download (fallback de ano só para o ano mais recente)
# =========================
def download_image_with_year_fallback(
    region: str, base_year: int, week: int, allow_year_fallback: bool
):
    """
    Tenta baixar o 'base_year'. Se for o ano mais recente e vier 'sem dados',
    tenta 2024, 2023... até achar. Anos anteriores não têm fallback.
    Retorna (Image, used_year) ou (None, None).
    """
    idx = years.index(base_year)
    year_list = years[idx:] if allow_year_fallback else [base_year]
    for yr in year_list:
        url = regions[region].format(year=yr, week=week)
        print(f"Baixando {region} - ano {yr} - semana {week}")
        try:
            r = requests.get(url, timeout=60)
        except requests.RequestException as e:
            print(f"[ERRO] Rede: {e}")
            continue
        if not is_image_response(r):
            print(f"[ERRO] Resposta inválida (status {r.status_code})")
            continue
        try:
            img = Image.open(BytesIO(r.content))
        except UnidentifiedImageError:
            print("[ERRO] Conteúdo não é imagem")
            continue
        if has_no_data_banner(img):
            print(f"[INFO] {region} {yr}: sem dados (banner).")
            if allow_year_fallback:
                continue
            return None, None
        return img, yr
    print(f"[WARN] Não achou dados para {region} partindo de {base_year}")
    return None, None


# =========================
# Execução principal
# =========================
initial_week = get_initial_week()
max_lookback = get_max_lookback()
week = find_available_week(initial_week, max_lookback)
print(f"[INFO] Semana usada: {week}")

# 1) Download por região/ano e combinação horizontal (2022→2025 à direita; 2025 à esquerda)
for region_name in regions.keys():
    imgs, used_years = [], []
    for i, year in enumerate(years):
        allow_year_fallback = i == 0  # ✅ somente para 2025
        img, used_year = download_image_with_year_fallback(
            region_name, year, week, allow_year_fallback
        )
        if img is None:
            print(f"[WARN] Ignorando {region_name} {year}")
            continue
        # >>> nomes FIXOS (sem semana) <<<
        path = os.path.join(output_dir, f"{region_name}_{used_year}.png")
        img.save(path)
        imgs.append(img)
        used_years.append(used_year)

    if imgs:
        combo = combine_images_horizontally(
            list(reversed(imgs))
        )  # direita→esquerda anos crescentes
        out = os.path.join(output_dir, f"{region_name}_combined.png")  # <<< nome fixo
        combo.save(out)
        print(
            f"[OK] Combinada: {out} | anos usados (esq→dir): {list(reversed(used_years))}"
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

print(f"[DONE] Semana final utilizada (log): {week}")

# Salva a semana em um arquivo para o GitHub Actions usar no assunto do e-mail
with open(os.path.join(output_dir, "week.txt"), "w") as f:
    f.write(str(week))
