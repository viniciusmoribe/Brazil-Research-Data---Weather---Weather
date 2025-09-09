
@echo off
echo ========================================
echo Inicializando ambiente do projeto...
echo ========================================

:: cria venv
python -m venv .venv

:: ativa venv
call .venv\Scripts\activate

:: instala pacotes do requirements.txt
pip install -r requirements.txt

:: instala extensoes do VS Code (se code.cmd estiver no PATH)
echo Instalando extensoes VS Code...
code --install-extension ms-python.python
code --install-extension ms-toolsai.jupyter

echo ========================================
echo Ambiente configurado com sucesso!
echo Ative o ambiente com:
echo     .venv\Scripts\activate
echo E inicie o VS Code com:
echo     code .
echo ========================================
pause
