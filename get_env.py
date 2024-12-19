import os
from dotenv import load_dotenv

load_dotenv(f'./.env')

def get_env():
    env_dict = {
        'logger_name': os.getenv('logger_name', 'DEFAULT')
        , 'logger_level': int( os.getenv('logger_level') )
        , 'telegram_bot_token': os.getenv('telegram_bot_token')
        , 'telegram_default_alert_roomID': os.getenv('telegram_default_alert_roomID')
        , 'company_conn': os.getenv('company_conn')
        , 'company_username': os.getenv('company_username')
        , 'company_password': os.getenv('company_password')
        , 'company_website': os.getenv('company_website')
        , 'company_landing_area_dir': os.getenv('company_landing_area_dir')
        , 'target_company_ETL_pipelines_dir': os.getenv('company_landing_area_dir')
    }
    
    return env_dict

