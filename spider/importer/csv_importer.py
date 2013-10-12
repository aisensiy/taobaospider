import csv
import types
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

class Table(object):
  def __init__(self, tablename, conn):
    self.tablename = tablename
    self.conn = conn
    self.counter = 0

  def __len__(self):
    return len(self.records)

  def insert_record(self, record):
    kvs = [[field, record[field]] for field, idx, cast, typename in self.getfields()]
    fields = ', '.join([kv[0] for kv in kvs])
    self.conn.execute \
      ("insert into %s (%s) values(%s)" % (self.tablename, fields, ', '.join(['%s'] * len(kvs))), [kv[1] for kv in kvs])

  def create_and_insert(self, filename, *args, **kvargs):
    self.create_table()
    self.insert_records_from_csv(filename, *args, **kvargs)

  def create_table(self):
    self.conn.execute(self.table_create_sql())

  def getfields(self, fields=[]):
    """ The fields is [] """
    newfields = []

    for field in fields:
      l = list(field)
      if len(field) == 3: l.append(None)
      newfields.append(l)

    return newfields

  def insert_records_from_csv(self, filename, skipheader=False, n=None, batch=300):
    print "[INFO]\tRead file:", filename
    fp = open(filename)
    counter = 0

    for i, line in enumerate(fp):
      if i == 0 and skipheader == True: continue
      if i == n: break
      record = self.makerecord(line, self.getfields())
      self.insert_record(record)
      counter += 1
      if counter % batch == 0:
        self.conn.commit()
        print "[INFO]\tcurrent counter: %d" % counter

    self.conn.commit()
    fp.close()
    print "[INFO]\tTotal:", counter

  def makerecord(self, line, fields):
    obj = {}

    rawdata = line.split(',')
    for field, index, cast, typename in fields:
      try:
        v = rawdata[index].strip()
        if len(v) == 0 or v == 'NULL': v = None
        else: v = cast(v)
      except Exception as ex:
        print "[ERROR]\t", ex, field, index
        v = None

      obj[field] = v

    return obj

  def table_create_sql(self):
    mapper = {
      "str": "text",
      "int": "int"
    }

    sql_template = "create table IF NOT EXISTS %s ( id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, %s ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"

    values = []
    for field, idx, cast, typename in self.getfields():
      if typename:
        values.append("%s %s" % (field, typename))
      else:
        values.append("%s %s" % (field, mapper[cast.__name__]))

    return sql_template % (self.tablename, ", ".join(values))

  def insertrecord_sql(self, record):
    """ Generate the sql according to the tablename and fields """
    kvs = [[field, record[field]] for field, idx, cast, typename in self.getfields()]

    def postprocess(v):
      if v == None: return 'NULL'
      else: return "'%s'" % str(v)

    return "insert into %s (%s) values (%s)" % \
      (self.tablename, ','.join([kv[0] for kv in kvs]), ','.join([postprocess(kv[1]) for kv in kvs]))

class Taobao(Table):
  def __init__(self, tablename):
    super(Taobao, self).__init__(tablename)

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

