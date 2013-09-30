import re
import gzip
from StringIO import StringIO

def str_sanitize(title):
  title = title.strip()
  title = re.sub(r'\s+', ' ', title)
  return title

def str_gzip(content):
  """
  Encode to utf8 and then gzip
  """
  stringio = StringIO()
  gzip_file = gzip.GzipFile(fileobj=stringio, mode='w')
  gzip_file.write(content.encode('utf8'))
  gzip_file.close()
  return stringio.getvalue()

