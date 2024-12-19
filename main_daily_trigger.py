import os
import csv
import re
import time
import shutil
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from zipfile import ZipFile
from get_env import get_env
from get_logger import get_logger, get_logger_format, get_datefmt, info_n_telegram_sendtext, warning_n_telegram_sendtext, error_n_telegram_sendtext
from logging.handlers import TimedRotatingFileHandler


# Get .env variables, logger, and logger formats
env_dict = get_env()
logger = get_logger()
logger_format = get_logger_format()
datefmt = get_datefmt()


# Define argument parser
parser = argparse.ArgumentParser(description='daily Scripts for company')

args = parser.parse_args()
# END argument parser


# MORE DETAILED EXPLANATION AT
# python main.py --help
# python main.py -h


if __name__ == "__main__":
    
    # .env variables
    logger_name = env_dict.get('logger_name')
    telegram_default_alert_roomID = env_dict.get('telegram_default_alert_roomID')
    company_landing_area_dir = env_dict.get('company_landing_area_dir')
    
    # Initialize today datetime
    today = datetime.today()
    today_iso = today.strftime('%Y-%m-%d')
    temp_date = today.strftime('%Y%m%d')
    temp_hour = today.strftime('%H%M%S')
    
    # Perform datetime calculations
    yesterday = today - timedelta(days=1)
    yesterday_iso = yesterday.strftime('%Y-%m-%d')
    
    first_day_this_month = today.replace(day=1)
    first_day_this_month_iso = first_day_this_month.strftime('%Y-%m-%d')
    
    # dateutil.relativedelta. day=31 will always always return the last day of the month:
    last_day_this_month = today + relativedelta(day=31)
    last_day_this_month_iso = last_day_this_month.strftime('%Y-%m-%d')
    
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    last_day_prev_month_iso = last_day_prev_month.strftime('%Y-%m-%d')
    
    first_day_prev_month = last_day_prev_month.replace(day=1)
    first_day_prev_month_iso = first_day_prev_month.strftime('%Y-%m-%d')
    
    # Change directory to target directory
    os.chdir( f'{company_landing_area_dir}' )
    
    output_path = f'{os.getcwd()}'.replace(f'\\', '/' )
    log_path = f'.././logs/{temp_date}/{temp_hour}/daily/'
    
    # Make directories IF NOT EXIST
    os.makedirs( output_path, exist_ok=True )
    os.makedirs( log_path , exist_ok=True )
    
    # Add logging handler
    handler = TimedRotatingFileHandler(f'{log_path}/daily_script_company', when='d', interval=1 )
    handler.namer = lambda name: name + ".log"
    handler.setLevel( env_dict.get('logger_level') )
    handler.setFormatter( logging.Formatter(logger_format, datefmt=datefmt) )
    logger.addHandler(handler)
    # END Add logging handler
    
    # Run the following commands SEQUENTIALLY
    try:
        os.system("taskkill /F /im EXCEL.EXE")

        # SHPdt
        os.system( f'python main.py -e SHPdt -fd "{first_day_this_month_iso}" -td "{last_day_this_month_iso}" -rid "{telegram_default_alert_roomID}"' )
    
        # SHPlt
        os.system( f'python main.py -e SHPlt -fd "{first_day_this_month_iso}" -td "{last_day_this_month_iso}" -rid "{telegram_default_alert_roomID}"' )

        # SHPdv
        os.system( f'python main.py -e SHPdv -fd "{first_day_this_month_iso}" -td "{last_day_this_month_iso}" -rid "{telegram_default_alert_roomID}"' )

        os.system("taskkill /im EXCEL.EXE")
        
    except Exception as e:
        error_n_telegram_sendtext(logger, f'{logger_name}|main_daily_company|Encounter error: {str(e)}', telegram_default_alert_roomID)

