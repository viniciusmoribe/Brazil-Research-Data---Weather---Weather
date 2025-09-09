@echo off
call .venv\Scripts\activate

echo === Cecafe download ===
python src\data\codigo3_baixar_excel_cecafe.py
if errorlevel 1 goto :error


echo === Rains webscraping ===
python src\data\codigo7_scrape_rain.py
if errorlevel 1 goto :error

echo === Min Temps webscraping ===
python src\data\codigo8_scrape_temp.py
if errorlevel 1 goto :error

echo === Concluido com sucesso ===
goto :eof

:error
echo Houve erro no processo.
exit /b 1