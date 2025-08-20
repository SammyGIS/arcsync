"""





"""


import logging


def setup_logger(log_file_dest:str="local", *args, **kwargs):
    """local_file_dest: 'local', 'cloud' or custom function"""

    logger = logging.getLogger("cdc_logger")
    logger.setLevel(logging.INFO)

    logger.handlers = []

    if log_file_dest == "local":
        fh = logging.FileHandler("cdc_logfile.log")
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
    elif log_file_dest =="cloud":
        ch = logging.FileHandler("cdc_logfile.log")
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler
    
    elif callable(log_file_dest):
        logger.addHandler(logging.StreamHandler())
        logger.info("Using custom log destination function.")
    
    else:
        raise ValueError("Invalid logfiledestiantion, choose 'local', 'cloud' or 'callable'")


