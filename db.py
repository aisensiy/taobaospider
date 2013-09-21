import sqlite3

class DB:
  def __init__(self, dbname):
    self.conn = sqlit3.connect(dbname)

  def create_tables(self):
    """docstring for create_tables"""
    create_url_sql = """
    create table url(
      id int(11) NOT NULL AUTO_INCREMENT,
      url text NOT NULL,
      content text
    )
    """
    create_url_index_sql = """
    create index urlidx on url(url)
    """
    create_rawdata_sql = """
    create table rawdata(
      id int(11) NOT NULL AUTO_INCREMENT,
      uid int,
      ip varchar(128),
      agent text,
      docsign
    )
    """


