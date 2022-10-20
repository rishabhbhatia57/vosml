import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date

def setup_custom_logger(name):
    today = date.today()
    # d3 = today.strftime("%Y-%m-%d")
    filename = "log.txt"

    logging.basicConfig(handlers=[RotatingFileHandler(filename, maxBytes=10485760, backupCount=5)],
                        level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s ')
    logger = logging.getLogger()
    return logger