import os
import yaml
import logging
import logging.config

def setup_logging(name, config_file = 'logging_config.yaml', default_level=logging.INFO):
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f.read())  
            set_filename(config, name)            
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)

def set_filename(config, name):
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f'{os.path.basename(os.path.splitext(name)[0])}.log')    
    config['handlers']['fileHandler']['filename'] = log_filename
