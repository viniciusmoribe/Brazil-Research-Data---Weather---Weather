import requests
from PIL import Image
from io import BytesIO
import os

# Cria diretório local para salvar imagens
output_dir = "vhi_images"
os.makedirs(output_dir, exist_ok=True)

# URLs por região (com placeholder para o ano)
regions = {
    "region_13": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
    "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
    "&provinceID=13&latlonRange=-22.922747,-51.045883,-14.233427,-39.856762"
    "&title=VHI%20of%20current%20year&type=VHI&week={year},35",
    "region_25": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
    "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
    "&provinceID=25&latlonRange=-25.303192,-53.109604,-19.779652,-43.859108"
    "&title=VHI%20of%20current%20year&type=VHI&week={year},35",
    "region_8": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
    "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
    "&provinceID=8&latlonRange=-21.297190,-41.878914,-17.891941,-39.6"
    "&title=VHI%20of%20current%20year&type=VHI&week={year},35",
    "region_22": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
    "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
    "&provinceID=22&latlonRange=-13.557581,-66.806473,-7.969309,-59.774288"
    "&title=VHI%20of%20current%20year&type=VHI&week={year},35",
    "region_5": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/imageMercator.php?"
    "&country=31,BRA&source=Blended&options=1,1,1,1,0,1,0,1,1"
    "&provinceID=5&latlonRange=-18.349859,-46.617046,-8.533636,-37.349030"
    "&title=VHI%20of%20current%20year&type=VHI&week={year},35",
}

# Anos (ordem da direita para a esquerda)
years = [2025, 2024, 2023, 2022]


# Funções para combinação mantendo escala (sem distorcer)
def combine_images_horizontally(images):
    max_height = max(img.height for img in images)
    total_width = sum(img.width for img in images)

    combined = Image.new("RGB", (total_width, max_height), (255, 255, 255))
    x_offset = 0
    for img in images:
        y_offset = (max_height - img.height) // 2
        combined.paste(img, (x_offset, y_offset))
        x_offset += img.width
    return combined


def combine_images_vertically(images):
    max_width = max(img.width for img in images)
    total_height = sum(img.height for img in images)

    combined = Image.new("RGB", (max_width, total_height), (255, 255, 255))
    y_offset = 0
    for img in images:
        x_offset = (max_width - img.width) // 2
        combined.paste(img, (x_offset, y_offset))
        y_offset += img.height
    return combined


# 1. Baixar imagens por região e ano
for region_name, url_template in regions.items():
    images = []
    for year in years:
        url = url_template.format(year=year)
        print(f"Baixando {region_name} - {year}")
        response = requests.get(url)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            img_path = os.path.join(output_dir, f"{region_name}_{year}.png")
            img.save(img_path)
            images.append(img)
        else:
            print(f"Erro ao baixar imagem de {region_name} {year}")

    if images:
        # 2. Combina horizontalmente os anos (2022 → 2025 da direita para esquerda)
        combined_img = combine_images_horizontally(list(reversed(images)))
        combined_path = os.path.join(output_dir, f"{region_name}_combined.png")
        combined_img.save(combined_path)
        print(f"Imagem combinada salva: {combined_path}")


# 3. Combinar verticalmente region_13 sobre region_25
def combinar_duas_regioes_verticais(region_cima, region_baixo, nome_arquivo_saida):
    path_cima = os.path.join(output_dir, f"{region_cima}_combined.png")
    path_baixo = os.path.join(output_dir, f"{region_baixo}_combined.png")

    if os.path.exists(path_cima) and os.path.exists(path_baixo):
        img_cima = Image.open(path_cima)
        img_baixo = Image.open(path_baixo)

        final_img = combine_images_vertically([img_cima, img_baixo])
        final_path = os.path.join(output_dir, nome_arquivo_saida)
        final_img.save(final_path)
        print(f"Imagem vertical combinada salva: {final_path}")
    else:
        print(
            f"Erro: uma ou ambas imagens não encontradas para {region_cima} e {region_baixo}"
        )


# 4. Execução das combinações verticais desejadas
combinar_duas_regioes_verticais("region_13", "region_25", "combined_region_13_25.png")
combinar_duas_regioes_verticais("region_22", "region_8", "combined_region_22_8.png")
