from csv_transformer import Table

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


if __name__ == '__main__':
  print open('../config/database.yml')
