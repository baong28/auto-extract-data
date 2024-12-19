import logging
import get_env
import requests
import requests.packages
from telegram.helpers import escape_markdown

env_dict = get_env.get_env()
logger_level = int( env_dict.get('logger_level', 20) )

def telegram_sendtext(message, roomID):
       
    send_text = 'https://api.telegram.org/bot' + env_dict.get('telegram_bot_token') + '/sendMessage?chat_id=' + roomID + '&parse_mode=Markdown&text=' + escape_markdown(message)

    response = requests.get(send_text, verify=False)

    return response.json()

def get_logger_format():
    logger_format = '%(asctime)s || %(name)s || %(levelname)s || %(message)s'   
    
    return logger_format


def get_datefmt():
    datefmt = '%Y-%m-%d %H:%M:%S'
    
    return datefmt

def get_logger():
    # Create a Formatter for formatting the log messages
    logger_format = get_logger_format()
    datefmt = get_datefmt()
    logging.basicConfig( format=logger_format, datefmt=datefmt )
    # Initialize logger
    logger = logging.getLogger(env_dict.get('logger_name') )

    # Level	    Numeric value
    # CRITICAL	50
    # ERROR	    40
    # WARNING	30
    # INFO	    20
    # DEBUG	    10
    # NOTSET	0
    logger.setLevel( logger_level )
    
    return logger


def debug_n_telegram_sendtext(logger, message, roomID, logger_level=logger_level):
    logger.debug( message )
    
    if logger_level>=10:
        msg_status = telegram_sendtext(message, roomID)
    
        return msg_status


def info_n_telegram_sendtext(logger, message, roomID, logger_level=logger_level):
    logger.info( message )
    
    if logger_level>=20:
        msg_status = telegram_sendtext(message, roomID)
    
        return msg_status


def warning_n_telegram_sendtext(logger, message, roomID, logger_level=logger_level):
    logger.warning( message )
    
    if logger_level>=30:
        msg_status = telegram_sendtext(message, roomID)
    
        return msg_status


def error_n_telegram_sendtext(logger, message, roomID, logger_level=logger_level):
    logger.error( message )
    
    if logger_level>=40:
        msg_status = telegram_sendtext(message, roomID)
    
        return msg_status


def critical_n_telegram_sendtext(logger, message, roomID, logger_level=logger_level):
    logger.critical( message )
    
    if logger_level>=50:
        msg_status = telegram_sendtext(message, roomID)
    
        return msg_status


