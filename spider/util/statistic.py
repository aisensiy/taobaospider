import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

from util.db import MySQL as DB
from util.tools import *
from config.settings import *

conn = DB(DB_CONFIG)

def statistic(conn):
  """
  Check 404 302 and normal content count
  """
  counter = {
    "404": 0,
    "302": 0,
    "other": 0
  }
  limit = 1000
  skip = 0

  while True:
    rows = conn.fetchall("select content from url where id <= %s and id > %s", (limit + skip, skip))
    if not len(rows): break
    skip += limit

    for row in rows:
      content = row[0]
      raw_content = str_ungzip(content)

      if raw_content in counter:
        counter[raw_content] += 1
      else:
        counter['other'] += 1

  return counter

if __name__ == '__main__':
  print statistic(conn)

