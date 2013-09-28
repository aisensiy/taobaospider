from csv_importer import Table
import sys

class TaobaoWithOutUser(Table):
  def __init__(self, tablename, conn):
    super(TaobaoWithOutUser, self).__init__(tablename, conn)

  def getfields(self):
    fields = [
      ('uid', 0, str),
      ('ip', 1, str),
      ('agent', 2, str),
      ('url', 5, str),
      ('site', 6, str),
      ('domain', 7, str),
      ('referurl', 8, str),
      ('date', 11, str, 'datetime'),
      ('staytime', 12, int),
      ('url_kw', 15, str),
      ('refer_kw', 16, str)
    ]

    return super(TaobaoWithOutUser, self).getfields(fields)


def import_taobao(filename):
  from config.settings import *
  from util.db import MySQL as DB
  conn = DB(DB_CONFIG)
  taobao = TaobaoWithOutUser('taobao', conn)
  taobao.create_and_insert(filename)
  conn.close()

if __name__ == '__main__':
  import time
  print "[INFO] Start at:", time.ctime()
  for filename in sys.argv[1:]:
    import_taobao(filename)
  print "[INFO] Finis at:", time.ctime()

