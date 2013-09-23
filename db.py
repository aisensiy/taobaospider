import sqlite3

class DB:
  def __init__(self, dbname):
    self.conn = sqlit3.connect(dbname)
    self.urltable = 'url'

  def create_tables(self):
    """docstring for create_tables"""
    create_url_sql = """
    create table %s(
      id int(11) NOT NULL PRIMARY KEY,
      url text NOT NULL,
      content text
    )
    """ % self.urltable

    create_url_index_sql = """
    create index urlidx on %s(url)
    """ % self.urltable

  def urlexists(self, url):
    cur = self.conn.execute("select * from %s where %s='%s'" % (self.urltable, 'url', url))
    res = cur.fetchone()
    return res != None

  def insert_url(self, url, content):
    self.conn.execute("insert into %s (url, content) values('%s', '%s')" % (self.urltable, url ,content))
    self.conn.commit()

  def fetch_rawdata(self, tablename, row, skip=0, limit=200):
    self.conn.execute("select %s from %s limit %d offset %d" % (row, tablename, limit, skip))

