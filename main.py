from csv_transformer import *
from db import MySQL as DB
import yaml
import sys

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
    content MEDIUMTEXT
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  """
  create_url_index_sql = """
  create index urlidx on url(url(255))
  """
  conn.execute(create_url_sql)
  conn.execute(create_url_index_sql)
  conn.commit()

if __name__ == '__main__':
  config = yaml.load(open('config/database.yml'))
  db = DB(config['development'])
  # create_taobao_table(sys.argv[1], db)
  create_tables(db)
  db.close()
