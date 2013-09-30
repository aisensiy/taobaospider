import re
import gzip
from StringIO import StringIO

def str_sanitize(title):
  title = title.strip()
  title = re.sub(r'\s+', ' ', title)
  return title

def str_gzip(content):
  stringio = StringIO()
  gzip_file = gzip.GzipFile(fileobj=stringio, mode='w')
  gzip_file.write(content)
  gzip_file.close()
  return gzip_file.getvalue()

