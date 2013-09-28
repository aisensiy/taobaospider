# -*- coding: utf-8 -*-

def url_content_heuristic(url, content, response):
  """ try to min storage """

  if content.find(u"很抱歉，您查看的宝贝不存在，可能已下架或者被转移。") > -1:
    content = "404"
  if response.geturl() != url:
    content = "302"

  return url, content

