from csv_importer import Table
import sys

class Region(Table):
  def __init__(self, tablename, conn, filedir=''):
    super(Region, self).__init__(tablename, conn)

  def getfields(self):
    fields = [
      ('city', 0, int),
      ('city_s', 1, str),
      ('province', 2, int, 'tinyint'),
      ('province_s', 3, str),
      ('region', 4, int, 'tinyint'),
      ('region_s', 5, str),
      ('tier', 6, int, 'tinyint'),
      ('tier_s', 7, str)
    ]

    return super(Region, self).getfields(fields)


def importfile(filename):
  from config.settings import *
  from util.db import MySQL as DB
  conn = DB(DB_CONFIG)
  taobao = Region('region', conn)
  taobao.create_and_insert(filename, batch=600, skipheader=True)
  conn.close()

if __name__ == '__main__':
  import time
  print "[INFO] Start at:", time.ctime()
  for filename in sys.argv[1:]:
    importfile(filename)
  print "[INFO] Finis at:", time.ctime()

