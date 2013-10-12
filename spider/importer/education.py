from csv_importer import Table
import sys

class Education(Table):
  def __init__(self, tablename, conn, filedir=''):
    super(Education, self).__init__(tablename, conn)

  def getfields(self):
    fields = [
      ('education', 0, int, 'tinyint'),
      ('education_s', 1, str)
    ]

    return super(Education, self).getfields(fields)


def importfile(filename):
  from config.settings import *
  from util.db import MySQL as DB
  conn = DB(DB_CONFIG)
  taobao = Education('education', conn)
  taobao.create_and_insert(filename, batch=600)
  conn.close()

if __name__ == '__main__':
  import time
  print "[INFO] Start at:", time.ctime()
  for filename in sys.argv[1:]:
    importfile(filename)
  print "[INFO] Finis at:", time.ctime()


