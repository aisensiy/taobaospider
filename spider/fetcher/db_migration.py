import yaml
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

from config.settings import *
from util.db import MySQL as DB

def create_taobao_table(filename, conn):
  """docstring for create_taobao_table"""
  taobao = Taobao('taobao')
  taobao.getrecords(filename)
  taobao.create_and_insert(conn)

def create_tables(conn):
  create_url_sql = """
  create table IF NOT EXISTS url(
    id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    url text NOT NULL,
    content MEDIUMTEXT,
    title text
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  """
  create_url_index_sql = """
  create index urlidx on url(url(255))
  """
  conn.execute(create_url_sql)
  conn.execute(create_url_index_sql)
  conn.commit()

if __name__ == '__main__':
  db = DB(DB_CONFIG)
  # create_taobao_table(sys.argv[1], db)
  create_tables(db)
  db.close()
