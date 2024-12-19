Stop-Process -Name 'excel.exe' -Force

# Daily Script
python main_daily_trigger.py

Stop-Process -Name 'excel.exe' -Force


