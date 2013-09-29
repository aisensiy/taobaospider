import MySQLdb

import logging
logging.basicConfig(level=logging.INFO)

class DB:
  """
  A vitual class
  """
  def __init__(self, config):
    pass

class MySQL(DB):
  def __init__(self, config):
    self.config = config
    self.connect()

  def connect(self):
    self.conn = MySQLdb.connect(**self.config)
    self.conn.ping(True)

  def ping(self, *args, **kvargs):
    self.conn.ping(*args, **kvargs)

  def execute(self, *args, **kvargs):
    try:
      cursor = self.conn.cursor()
      cursor.execute(*args, **kvargs)
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cursor = self.conn.cursor()
      cursor.execute(*args, **kvargs)
    return cursor

  def fetchone(self, sql, *args, **kvargs):
    logging.info("[SQL]: %s %s %s", sql, args, kvargs)
    cursor = self.conn.cursor()
    cursor.execute(sql, *args, **kvargs)
    return cursor.fetchone()

  def fetchall(self, sql, *args, **kvargs):
    logging.info("[SQL]: %s %s %s", sql, args, kvargs)
    cursor = self.conn.cursor()
    cursor.execute(sql, *args, **kvargs)
    return cursor.fetchall()

  def close(self):
    self.conn.commit()
    self.conn.close()

  def commit(self):
    return self.conn.commit()


if __name__ == '__main__':
  import yaml
  config = yaml.load(open('config/database.yml'))
  db = MySQL(config['development'])
  print db.fetchrows('taobao', 'url', 0, 10)
