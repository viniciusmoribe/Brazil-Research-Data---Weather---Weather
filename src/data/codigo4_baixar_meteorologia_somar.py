from datetime import date
from pathlib import Path

url = "https://somarmeteorologia.com.br/v3/excel/po_graf_pont.php?cid=BarradoChoca-BA,Ibicoara-BA,Itabela-BA,VitoriadaConquista-BA,AfonsoClaudio-ES,Alegre-ES,Brejetuba-ES,CachoeirodeItapemirim-ES,Colatina-ES,Iuna-ES,Jaguare-ES,Linhares-ES,MunizFreire-ES,NovaVenecia-ES,RioBananal-ES,SaoGabrieldaPalha-ES,VargemAlta-ES,Alfenas-MG,Araguari-MG,BoaEsperanca-MG,CaboVerde-MG,CamposAltos-MG,CamposGerais-MG,Caratinga-MG,CarmodoParanaiba-MG,Coromandel-MG,EsperaFeliz-MG,Guaxupe-MG,Lajinha-MG,Lambari-MG,Lavras-MG,Machado-MG,Manhuacu-MG,Matipo-MG,MonteCarmelo-MG,Nepomuceno-MG,NovaResende-MG,PatosdeMinas-MG,Patrocinio-MG,PocosdeCaldas-MG,SantaMargarida-MG,SantoAntoniodoAmparo-MG,SaoGotardo-MG,SaoSebastiaodoParaiso-MG,TresPontas-MG,Varginha-MG,Apucarana-PR,Jacarezinho-PR,Londrina-PR,NovaFatima-PR,Ariquemes-RO,Cacoal-RO,Altinopolis-SP,EspiritoSantodoPinhal-SP,Franca-SP,Garca-SP,Mococa-SP,Pedregulho-SP&ano=2025&mes=8"

# --- Download com Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import shutil

# Configurar pasta de download
download_dir = str(Path.cwd())
chrome_options = Options()
chrome_options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    },
)
chrome_options.add_argument(
    "--headless=new"
)  # Remova esta linha se quiser ver o navegador
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=chrome_options
)
driver.get(url)

# Espera o download (ajuste o tempo se necessário)
time.sleep(15)

# Renomeia o arquivo baixado
files = list(Path(download_dir).glob("*.xls*"))
if files:
    novo_nome = f"dados_meteorologicos_{date.today().isoformat()}.xls"
    shutil.move(str(files[0]), novo_nome)
    print(f"Arquivo salvo como {novo_nome}")
else:
    print("Falha ao baixar o arquivo com Selenium")

driver.quit()

# --- Fallback: requests (pode não funcionar devido a SSL) ---
import requests

try:
    response = requests.get(url, timeout=720, verify=False)
    if response.status_code == 200:
        filename = f"dados_meteorologicos_{date.today().isoformat()}.xls"
        Path(filename).write_bytes(response.content)
        print(f"Arquivo salvo como {filename}")
    else:
        print("Falha ao baixar o arquivo")
except Exception as e:
    print(f"Erro ao tentar baixar o arquivo: {e}")
