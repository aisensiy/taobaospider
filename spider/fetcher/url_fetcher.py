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
from threading import Thread
from util.db import MySQL as DB

from pyquery import PyQuery as pq
import re

import heuristic
from util.tools import *

# global
queue = Queue()
skip = 0

class UrlHandler:
  def __init__(self, conn):
    self.conn = conn

    self._fetchrows()

  def indexed(self, url):
    u = self.conn.fetchone \
        ("select * from url where `url` = '%s'" % url)

    if u != None: return True
    else: return False

  def empty(self):
    return queue.empty()

  def get_url(self):
    url = queue.get()
    if queue.qsize() < 50:
      self._fetchrows()

    try:
      url = url.decode('utf8')
    except Exception, e:
      try:
        url = url.decode('gbk')
      except Exception, e:
        print '[ERROR] decode url failed'

    return url

  def insert_url(self, url, content):
    try:
      dom = pq(content)
      title = dom('title') and dom('title')[0].text or None
      if title: title = title_sanitize(title)
    except Exception, e:
      print e
      print "[ERROR] parse html error in", url
      title = None

    self.conn.execute \
        ("insert into url(url, content, title) values(%s, %s, %s)", (url, content, title))
    self.conn.commit()

  # private
  def _fetchrows(self, limit=1000):
    """
    Fetch many rows with one column
    """
    global skip
    rows = self.conn.fetchall("select url from taobao limit %d offset %d" % (limit, skip))
    rows = set(rows)
    skip += limit
    for row in rows:
      if row[0]: queue.put(row[0])

    print '[FETCH] urls from db, now queue size is: ', queue.qsize()

class Worker(Thread):
  def __init__(self, url_handler):
    super(Worker, self).__init__()
    self.url_handler = url_handler

  def run(self):
    while not self.url_handler.empty():
      url = self.url_handler.get_url()
      print '[GET] url: ', url

      if url and not self.url_handler.indexed(url):
        self.fetch_url(url)

  def fetch_url(self, url):
    request = urllib2.Request(url)
    request.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.76 Safari/537.36')
    request.add_header('Accept-Encoding', 'gzip,deflate')

    content = None
    try:
      print '[REQEST] url: ', url
      response = urllib2.urlopen(request)
      content = response.read()
      if response.info().getheader('Content-Encoding') \
        and response.info().getheader('Content-Encoding') == 'gzip':
        content = GzipFile(fileobj=StringIO(content)).read()

      content = self._decode_content(content)
    except urllib2.URLError as e:
      print "[ERROR]", type(e)    #catched
    except socket.timeout as e:
      print "[ERROR]", type(e)    #catched
    except Exception as e:
      print "[ERROR] Fetch url", e

    if not content: return
    url, content = heuristic.url_content_heuristic(url, content, response)
    # TODO: save it
    self.url_handler.insert_url(url, content)
    print '[INSERT] url: ', url

  def _decode_content(self, content):
    # decode from gbk or utf8
    newcontent = None
    try:
      newcontent = content.decode('gbk')
    except Exception, e:
      print "[INFO] failed ", e
      try:
        newcontent = content.decode('utf8')
      except Exception, e:
        print "[INFO] failed ", e
        print "[ERROR] %s cannot decode by gbk or utf8" % url

    return newcontent

class TaskManager():
  def __init__(self, dbconfig, thread_num=10):
    self.dbconfig = dbconfig
    self.thread_num = thread_num

  def start(self):
    for num in range(self.thread_num):
      Worker(UrlHandler(DB(self.dbconfig))).start()

if __name__ == '__main__' or True:
  from config.settings import *
  # print DB_CONFIG
  tm = TaskManager(DB_CONFIG, thread_num=5)
  tm.start()
