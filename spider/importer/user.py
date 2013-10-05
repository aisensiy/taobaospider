from csv_importer import Table
import sys

class User(Table):
  def __init__(self, tablename, conn):
    super(User, self).__init__(tablename, conn)

  def getfields(self):
    fields = [
      ('uid', 0, str),
      ('gender', 2, int),
      ('city', 3, int),
      ('income_pre', 5, int),
      ('income_fml', 6, int),
      ('education', 7, int),
      ('job', 8, int),
      ('industry', 9, int),
      ('birth', 10, str)
    ]

    return super(User, self).getfields(fields)


def import_taobao(filename):
  from config.settings import *
  from util.db import MySQL as DB
  conn = DB(DB_CONFIG)
  taobao = User('users', conn)
  taobao.create_and_insert(filename, batch=600)
  conn.close()

if __name__ == '__main__':
  import time
  print "[INFO] Start at:", time.ctime()
  for filename in sys.argv[1:]:
    import_taobao(filename)
  print "[INFO] Finis at:", time.ctime()


