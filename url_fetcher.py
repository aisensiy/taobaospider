import urllib2
from Queue import Queue
from threading import Thread
from db import MySQL as DB

class UrlHandler:
  def __init__(self, conn, skip = 0):
    self.queue = Queue()
    self.conn = conn
    self.skip = skip

    self._fetchrows()

  def indexed(self, url):
    u = self.conn.fetchone \
      ("select * from url where `url` = '%s'" % url)

    if u != None: return True
    else: return False

  def empty(self):
    return self.queue.empty()

  def get_url(self):
    url = self.queue.get()
    if self.queue.qsize() < 10:
      self._fetchrows()

  def insert_url(self, url, content):
    self.conn.execute \
      ("insert into url(url, content) values('%s', '%s')", (url, content))
    self.conn.commit()

  # private
  def _fetchrows(self, limit=1000):
    """
    Fetch many rows with one column
    """
    rows = self.conn.fetchall("select url from taobao limit %d offset %d" % (limit, self.skip))
    rows = set(rows)
    self.skip += limit
    for row in rows:
      self.queue.put(row[0])

    print 'Fetch urls from db, now queue size is: ', self.queue.qsize()

class Worker(Thread):
  def __init__(self, url_handler):
    super(Worker, self).__init__()
    self.url_handler = url_handler

  def run(self):
    while not self.url_handler.empty():
      url = self.url_handler.get_url()
      print 'Get url: ', url

      if url and not self.url_handler.indexed(url):
        self.fetch_url(url)

  def fetch_url(self, url):
    print '==================== fetch url: ', url
    lines = urllib2.urlopen(url).read()
    # TODO: save it
    self.url_handler.insert_url(url, lines)
    print '==================== url chars: ', url, lines.split('\n')[0]

class TaskManager():
  def __init__(self, url_handler, thread_num=10):
    self.url_handler = url_handler
    self.thread_num = thread_num

  def start(self):
    for num in range(self.thread_num):
      Worker(self.url_handler).start()

if __name__ == '__main__' or True:
  import yaml
  config = yaml.load(open('config/database.yml'))
  url_handler = UrlHandler(DB(config['development']))
  tm = TaskManager(url_handler, thread_num=1)
  tm.start()
