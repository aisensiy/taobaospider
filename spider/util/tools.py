import re

def title_sanitize(title):
  title = title.strip()
  title = re.sub(r'\s+', ' ', title)
  return title

