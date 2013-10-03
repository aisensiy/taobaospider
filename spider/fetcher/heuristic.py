# -*- coding: utf-8 -*-
import logging

logging.basicConfig(level=logging.INFO)

def url_content_heuristic(url, content, response):
  """ try to min storage """

  newcontent = None
  try:
    if content.find(u"很抱歉，您查看的宝贝不存在，可能已下架或者被转移。") > -1:
      newcontent = "404"
    elif response.geturl() != url:
      newcontent = "302"
  except Exception, e:
    logging.warn("heuristic [%s] [%s]", e, url)

  return newcontent

