import os
import yaml

# ROOT/settings.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIG_ROOT = os.path.join(PROJECT_ROOT, 'config')
DB_CONFIG_FILE = os.path.join(CONFIG_ROOT, 'database.yml')

DB_CONFIG = yaml.load(open(DB_CONFIG_FILE).read())['development']

# TO use yaml
# import settings
# xxx.load(settings.DB_CONFIG_FILE)
