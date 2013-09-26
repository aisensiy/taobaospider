import MySQLdb

class DB:
  """
  A vitual class
  """
  def __init__(self, config):
    pass

class MySQL(DB):
  def __init__(self, config):
    self.conn = MySQLdb.connect(**config)

  def execute(self, sql):
    cursor = self.conn.cursor()
    return cursor.execute(sql)

  def fetchone(self, sql):
    print "run: ", sql
    cursor = self.conn.cursor()
    cursor.execute(sql)
    return cursor.fetchone()

  def fetchall(self, sql):
    print "run: ", sql
    cursor = self.conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

  def close(self):
    self.conn.close()

  def commit(self):
    return self.conn.commit()


if __name__ == '__main__':
  import yaml
  config = yaml.load(open('config/database.yml'))
  db = MySQL(config['development'])
  print db.fetchrows('taobao', 'url', 0, 10)
