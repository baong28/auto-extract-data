import pandas as pd
import csv
import argparse
import logging
import time, os, json, re
import datetime as dt
import itertools
from get_env import get_env
from get_logger import get_logger, get_logger_format, get_datefmt, info_n_telegram_sendtext, warning_n_telegram_sendtext, error_n_telegram_sendtext
from CompanyExport import CompanyExport
from logging.handlers import TimedRotatingFileHandler


# Get .env variables, logger, and logger formats
env_dict = get_env()
logger = get_logger()
logger_format = get_logger_format()
datefmt = get_datefmt()

# Get xpath.json , export flow
xpaths = json.load(open('export.json'))

# Define argument parser
parser = argparse.ArgumentParser(description='Process various company Export ETL functions')

parser.add_argument('-e', '--export-type', dest='export_type', help="company branch name i.e. SHP, SBT", type=str, required=True)

# parser.add_argument('-rp', '--report-type', dest='report_type', help="report type i.e. Revenue, Regimen, Services,...", type=str, required=True)

parser.add_argument('-fd', '--from-date', dest='from_date', help="Input from date in the format YYYY-MM-DD", type=str)

parser.add_argument('-td', '--to-date', dest='to_date', help="Input to date in the format YYYY-MM-DD", type=str)

parser.add_argument('-d', '--delimiter', dest='delimiter', help="Input delimiter for .csv output files i.e. ',' 'tab' ';' '|' etc. Default: ','", default=',', type=str)

parser.add_argument('-rid', '--roomID', dest='roomID', help="Telegram roomID to which to send alerts i.e. '-123456789'. Default roomID 'FCV_DWH_SAP_ETL_Pipelines'", default=env_dict.get('telegram_default_alert_roomID'), type=str)

parser.add_argument('-cd', '--cur-date', dest='cur_date', help="Input current TARGET date for landing_area in the format YYYY-MM-DD. Default cur_date 'TODAY'", default=pd.to_datetime("today").strftime('%Y-%m-%d'), type=str)

args = parser.parse_args()
# END argument parser

# MORE DETAILED EXPLANATION AT
# python main.py --help
# python main.py -h

if __name__ == "__main__":
    
    # .env variables
    company_conn = env_dict.get('company_conn')
    company_username = env_dict.get('company_username')
    company_password = env_dict.get('company_password')
    company_website = env_dict.get('company_website')
    company_landing_area_dir = env_dict.get('company_landing_area_dir')

    # xpath website
    usernameTxtBox = xpaths['usernameTxtBox']
    passwordTxtBox = xpaths['passwordTxtBox']
    submitButton   = xpaths['submitButton']

    # Shared variables
    export_type = args.export_type.replace(' ', '')
    from_date = args.from_date
    to_date = args.to_date
    delimiter = args.delimiter if args.delimiter != 'tab' else '\t'
    roomID = args.roomID
    
    cur_date = pd.to_datetime( args.cur_date ) if args.cur_date is not None else pd.to_datetime("today")
    temp_date = cur_date.strftime('%Y%m%d')
    
    # Define useful variables
    output_path = f'{company_landing_area_dir}/{temp_date}/company/{export_type}'
    log_path = f'{company_landing_area_dir}/{temp_date}/logs/company/{export_type}'
    csv_path = f'{output_path}/'
    archive_path = f'{output_path}/archive/'
    
    # Define xlsx_kwargs 
    # For tcodes exporting .xlsx files
    xlsx_kwargs = {
        'delimiter': delimiter          # ',' 'tab' ';' '|' etc. for out file .csv
        , 'quoting': csv.QUOTE_ALL      # csv.QUOTE_NONE csv.QUOTE_MINIMAL csv.QUOTE_NONNUMERIC csv.QUOTE_ALL
        , 'outputencoding': 'utf-8'
    }
    
    # Define to_csv_kwargs
    # For tcodes exporting NON .xlsx files
    to_csv_kwargs = {
        'sep': delimiter
        , 'header': True
        , 'index': False                # Do not write row names
        , 'encoding': 'utf-8'
        , 'quoting': csv.QUOTE_ALL      # csv.QUOTE_NONE csv.QUOTE_MINIMAL csv.QUOTE_NONNUMERIC csv.QUOTE_ALL
    }
    
    # Make directories IF NOT EXIST
    os.makedirs( output_path, exist_ok=True )
    os.makedirs( log_path , exist_ok=True )
    os.makedirs( csv_path , exist_ok=True )
    os.makedirs( archive_path , exist_ok=True )
    
    # Add logging handler
    handler = TimedRotatingFileHandler(f'{log_path}/company_export_{export_type}', when='d', interval=1 )
    handler.namer = lambda name: name + ".log"
    handler.setLevel( env_dict.get('logger_level') )
    handler.setFormatter( logging.Formatter(logger_format, datefmt=datefmt) )
    logger.addHandler(handler)
    # END Add logging handler
    
    logger.info( f'{export_type} || Creating desired paths IF NOT YET exist' )

    # Initialize companyExport Class
    company_export = companyExport( company_conn, company_website, company_username, company_password, company_landing_area_dir, export_type, from_date, to_date, output_path, csv_path, usernameTxtBox, passwordTxtBox, submitButton, archive_path, xlsx_kwargs, to_csv_kwargs, logger, roomID )
    
    info_n_telegram_sendtext(logger, f'{export_type} || main || Begin running', roomID)
    
##############################################################################################################################################
## FIRST, check IF azcopy_flag is True conditions
## if azcopy_flag:
##   pass
## 
##   # AZCopy Copy
##   # sap_export.azcopy_copy()
## 
## # SECOND, check IF csv_flag is True conditions
## elif csv_flag and re.match( '^(?:MB5B|COID)$', export_type, flags=re.IGNORECASE ) is None:
##    company_export.to_csv_convert_pipeline()
## 
## elif csv_flag and re.match( '^(?:MB5B|COID)$', export_type, flags=re.IGNORECASE ) is not None:
##    # tcode MB5B,COID input file is .txt tab-separated
##    # Thus, define read_csv_kwargs and to_csv_kwards
##    read_csv_kwargs = {
##        'sep': '\t'
##        , 'header': 0
##        , 'thousands': ','
##    }
##    company_export.to_csv_convert_pipeline( read_csv_kwargs, to_csv_kwargs )
##############################################################################################################################################    
    
    # Main program
    # if clauses TO BE ADDED HERE AS WE EXPAND to other company export functions
    if re.match( '^SHPdt$', export_type, flags=re.IGNORECASE ) is not None:
        # Apply SHP pipeline - Extract Revenue data
        company_export.SHP_Revenue_Extract_Pipelines()

    elif re.match( '^SHPlt$', export_type, flags=re.IGNORECASE ) is not None:
        # Apply SHP pipeline - Extract Regimen data
        company_export.SHP_Regimen_Extract_Pipelines()

    elif re.match( '^SHPdv$', export_type, flags=re.IGNORECASE ) is not None:
        # Apply SHP pipeline - Extract Services data
        company_export.SHP_Service_Extract_Pipelines()

    # elif re.match( '^SBT$', export_type, flags=re.IGNORECASE ) is not None:
    #     # Apply SBT pipeline
    #     company_export.SHP_Service_Extract_Pipelines()

    else:
        warning_n_telegram_sendtext(logger, f'{export_type} || main || Warning! NO matching export_type function FOUND', roomID)
    
    info_n_telegram_sendtext(logger, f'{export_type} || main || Finish running', roomID)
        
