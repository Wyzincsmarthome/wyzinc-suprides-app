@echo off
setlocal
cd /d "C:\Users\tiago\Desktop\amazon-csv-visiotech-dup2509 (2)\amazon-csv-visiotech-dup2509\amazon-csv-visiotech-dup"
if not exist logs mkdir logs
echo [%%date%% %%time%%] START >> "logs\suprides_sync.log"
".venv\Scripts\python.exe" -u run_suprides_sync.py >> "logs\suprides_sync.log" 2>&1
echo [%%date%% %%time%%] END   >> "logs\suprides_sync.log"
endlocal
