from csv_importer import Table
import sys

class Age(Table):
  def __init__(self, tablename, conn, filedir=''):
    super(Age, self).__init__(tablename, conn)

  def getfields(self):
    fields = [
      ('age', 0, int, 'tinyint'),
      ('age_s', 1, str)
    ]

    return super(Age, self).getfields(fields)


def importfile(filename):
  from config.settings import *
  from util.db import MySQL as DB
  conn = DB(DB_CONFIG)
  taobao = Age('ages', conn)
  taobao.create_and_insert(filename, batch=600)
  conn.close()

if __name__ == '__main__':
  import time
  print "[INFO] Start at:", time.ctime()
  for filename in sys.argv[1:]:
    importfile(filename)
  print "[INFO] Finis at:", time.ctime()


