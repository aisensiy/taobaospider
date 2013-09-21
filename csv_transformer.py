import csv
import types
import sys

class Table(object):
  def __init__(self):
    self.records = []

  def __len__(self):
    return len(self.records)

  def readfile(self, data_file, fields, skipheader=True, n=None):
    fp = open(data_file)

    for i, line in enumerate(fp):
      if i == 0 and skipheader == True:
        continue
      if i == n:
        break
      record = self.makerecord(line, fields)
      self.addrecord(record)

    fp.close()

  def makerecord(self, line, fields):
    obj = {}

    rawdata = line.split(',')
    for field, index, cast, typename in fields:
      try:
        v = rawdata[index]
        v = cast(v)
      except Exception as ex:
        print ex
        print line
        print field, index
        v = None

      obj[field] = v

    return obj

  def addrecord(self, record):
    self.records.append(record)

  def table_create_sql(self, tablename, fields):
    mapper = {
      "str": "text",
      "int": "int"
    }

    sql_template = """
create table %s(
  id int NOT NULL AUTO_INCREMENT,
%s
)
    """

    values = []
    for field, idx, cast, typename in fields:
      if typename:
        values.append("%s %s" % (field, typename))
      else:
        values.append("%s %s" % (field, mapper[cast.__name__]))

    return sql_template % (tablename, ",\n".join(values))

class Taobao(Table):
  def getrecords(self, filename, skipheader=True, n=None):
    self.readfile(filename, self.getfields(), skipheader, n)

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
      ('refer_kw', 16, str),
      ('raw_id', 18, int),
      ('gener', 21, int),
      ('age', 22, int),
      ('city', 23, int),
      ('income_pre', 24, int),
      ('income_fml', 25, int),
      ('education', 26, int),
      ('job', 27, int),
      ('industry', 28, int),
      ('birth', 29, str)
    ]

    newfields = []

    for field in fields:
      l = list(field)
      if len(field) == 3: l.append(None)
      newfields.append(l)

    return newfields

def main(filename):
  taobao = Taobao()
  print taobao.table_create_sql('taobao', taobao.getfields())
  taobao.getrecords(filename, skipheader=True, n=20)
  print len(taobao)
  print taobao.records[0]

if __name__ == '__main__':
  print sys.argv
  main(*sys.argv[1:])

