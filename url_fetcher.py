import urllib2
from Queue import Queue
from threading import Thread


# global var
remaind_urls = Queue()
fetched_urls = set()


class Worker(Thread):
  def run(self):
    while True:
      url = remaind_urls.get()
      if not url in fetched_urls:
        self.fetch_url(url)

  def fetch_url(self, url):
    lines = urllib2.urlopen(url).read()
    # TODO: save it
    print '==================== url chars: ', url, lines.split('\n')[0]


class TaskManager(object):
  def __init__(self, thread_num=10, *args, **kwargs):
    self.thread_num = thread_num

    self.load_fetched_url()
    self.load_remaind_url()

  def load_fetched_url(self):
    # TODO: load saved url from db
    for i in range(2, 6):
      fetched_urls.add('http://www.baidu.com/%s' % i)

  def load_remaind_url(self):
    # TODO: load remaind from db
    for i in range(100):
      remaind_urls.put('http://www.baidu.com/%s' % i)

  def start(self):
    for num in range(self.thread_num):
      Worker().start()

if __name__ == '__main__' or True:
  tm = TaskManager(thread_num=10)
  tm.start()
