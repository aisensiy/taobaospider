# -*- coding: utf-8 -*-

import sys
from nose.tools import *
import nose

from spider.fetcher.url_fetcher import UrlFetcher

class TestUrlFetcher:
  def setUp(self):
    self.fetcher = UrlFetcher()

  def test_fetch(self):
    # test gbk
    url = 'http://www.taobao.com/'
    assert self.fetcher.fetch(url) != None

    # test utf8
    url = 'http://movie.douban.com/subject/3718424/'
    assert self.fetcher.fetch(url) != None

    # test not gbk or utf8
    url = 'http://logo.taobao.com/shop-logo/da/f9/T1gFecXzVXXXb1upjX'
    assert self.fetcher.fetch(url) == None

    # test 404
    url = 'http://item.taobao.com/auction/noitem.htm?itemid=123123123123&catid=0'
    assert self.fetcher.fetch(url) == '404'

    # test 302
    url = 'http://i.taobao.com/my_taobao.htm?spm=1.6659421.754894437.1.74Alch&jlogid=p282311494418d'
    assert self.fetcher.fetch(url) == '302'

if __name__ == '__main__':
  result = nose.run()
  print result
