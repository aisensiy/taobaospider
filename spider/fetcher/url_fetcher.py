# -*- coding: utf-8 -*-

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(PROJECT_ROOT)

import urllib2
from gzip import GzipFile
from StringIO import StringIO
from threading import Thread
import socket

from Queue import Queue
from threading import Thread, Lock
from util.db import MySQL as DB

from pyquery import PyQuery as pq

import heuristic
from util.tools import *

from time import ctime

import logging

logging.basicConfig(level=logging.INFO)
urllib2.socket.setdefaulttimeout(20)

# global
queue = Queue()
skip = 0
counter = 0
lock = Lock()

class UrlHandler:
  def __init__(self, conn):
    self.conn = conn
    self._fetchrows()

  def indexed(self, url):
    u = self.conn.fetchone \
          ("select * from url where `url` = %s", url)

    if u != None: return True
    else: return False

  def empty(self):
    return queue.empty()

  def get_url(self):
    url = queue.get()

    global counter
    with lock:
      counter += 1

    logging.info('url count %d', counter)

    with lock:
      if queue.qsize() < 50: self._fetchrows()

    try:
      url = url.decode('utf8')
    except Exception, e:
      try:
        url = url.decode('gbk')
      except Exception, e:
        logging.warn('decode url failed')
        url = None

    return url

  def insert_url(self, url, content):
    try:
      dom = pq(content)
      title = dom('title') and dom('title')[0].text or None
      if title: title = str_sanitize(title)
    except Exception, e:
      logging.warn(e)
      logging.warn("Parse html error in %s", url)
      title = None

    gz_content = str_gzip(str_sanitize(content))

    self.conn.execute \
        ("insert into url(url, content, title) values(%s, %s, %s)", (url, gz_content, title))
    self.conn.commit()

  # private
  def _fetchrows(self, limit=1000):
    """
    Fetch many rows with one column
    """
    global skip
    rows = self.conn.fetchall("select url from taobao limit %s offset %s", (limit, skip))
    rows = set(rows)
    if not len(rows): return
    skip += limit
    for row in rows:
      if row[0]: queue.put(row[0])

    logging.info('[FETCH] now qsize: %d', queue.qsize())

class UrlFetcher():
  def fetch(self, url):
    request = urllib2.Request(url)
    request.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.76 Safari/537.36')
    request.add_header('Accept-Encoding', 'gzip,deflate')

    content = None
    try:
      logging.info('[REQ] %s url: %s', ctime(), url)
      response = urllib2.urlopen(request)
      content = response.read()
      if response.info().getheader('Content-Encoding') \
        and response.info().getheader('Content-Encoding') == 'gzip':
        content = GzipFile(fileobj=StringIO(content)).read()

      content = self._decode_content(content, url)
    except Exception as e:
      logging.warn("%s", e)

    if content: content = heuristic.url_content_heuristic(url, content, response)
    return content

  def _decode_content(self, content, url):
    # decode from gbk or utf8
    newcontent = None
    try:
      newcontent = content.decode('gbk')
    except Exception, e:
      logging.warn("[INFO] failed %s", e)
      try:
        newcontent = content.decode('utf8')
      except Exception, e:
        logging.warn("[INFO] failed %s", e)
        logging.warn("[ERROR] %s cant decode by gbk or utf8", url)

    return newcontent


class Worker(Thread):
  def __init__(self, url_handler, url_fetcher):
    super(Worker, self).__init__()
    self.url_handler = url_handler
    self.url_fetcher = url_fetcher

  def run(self):
    while True:
      url = self.url_handler.get_url()
      # logging.info('[GET] url: %s', url)

      if url != None and not self.url_handler.indexed(url):
        content = self.url_fetcher.fetch(url)
        if not content: continue
        # TODO: save it
        self.url_handler.insert_url(url, content)
        logging.info('[INSERT] %s', url)
      queue.task_done()


class TaskManager():
  def __init__(self, dbconfig, thread_num=10):
    self.dbconfig = dbconfig
    self.thread_num = thread_num

  def start(self):
    for num in range(self.thread_num):
      worker = Worker(UrlHandler(DB(self.dbconfig)), UrlFetcher())
      worker.setDaemon(True)
      worker.start()

if __name__ == '__main__' or True:
  logging.info('start at %s', ctime())
  from config.settings import *
  tm = TaskManager(DB_CONFIG)
  tm.start()
  queue.join()
  logging.info('finish at %s', ctime())
