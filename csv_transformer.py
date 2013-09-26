import csv
import types
import sys

class Table(object):
  def __init__(self, tablename):
    self.tablename = tablename
    self.records = []

  def __len__(self):
    return len(self.records)

  def getrecords(self, filename, skipheader=True, n=None):
    self.readfile(filename, self.getfields(), skipheader, n)
    print "Len of records: %d" % len(self)

  def create_and_insert(self, conn, batch=100):
    cnt = 0
    conn.execute(self.table_create_sql())
    conn.commit()
    print "total: %d" % len(self)
    for record in self.records:
      print "cnt: %d" % cnt
      conn.execute(self.insertrecord_sql(record))
      cnt += 1
      if cnt % batch == 0:
        conn.commit()

    conn.commit()

  def getfields(self, fields=[]):
    """
    The fields is []
    """
    newfields = []

    for field in fields:
      l = list(field)
      if len(field) == 3: l.append(None)
      newfields.append(l)

    return newfields


  def readfile(self, data_file, fields, skipheader=True, n=None):
    print "Read file: %s" % data_file
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
        print ex, field, index
        v = None

      obj[field] = v

    return obj

  def addrecord(self, record):
    self.records.append(record)

  def table_create_sql(self):
    mapper = {
      "str": "text",
      "int": "int"
    }

    sql_template = "create table IF NOT EXISTS %s ( id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, %s ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"

    values = []
    for field, idx, cast, typename in self.getfields():
      if typename:
        values.append("%s %s" % (field, typename))
      else:
        values.append("%s %s" % (field, mapper[cast.__name__]))

    return sql_template % (self.tablename, ", ".join(values))

  def insertrecord_sql(self, record):
    """
    Generate the sql according to the tablename and fields
    """
    kvs = [[field, record[field]] for field, idx, cast, typename in self.getfields()]

    def postprocess(v):
      if v == None: return 'NULL'
      else: return "'%s'" % str(v)

    return "insert into %s (%s) values (%s)" % \
      (self.tablename, ','.join([kv[0] for kv in kvs]), ','.join([postprocess(kv[1]) for kv in kvs]))


class Taobao(Table):
  def __init__(self, tablename):
    super(Taobao, self).__init__(tablename)
    self.tablename = tablename

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

    return super(Taobao, self).getfields(fields)

def main(filename):
  taobao = Taobao('taobao')
  print taobao.table_create_sql()
  taobao.getrecords(filename, skipheader=True)
  print len(taobao)
  print taobao.records[0]
  record = taobao.records[0]
  record['birth'] = None
  print taobao.insertrecord_sql(record)
  # import sqlite3
  # conn = sqlite3.connect('test.db')
  # taobao.create_and_insert(conn)

if __name__ == '__main__':
  print sys.argv
  main(*sys.argv[1:])

