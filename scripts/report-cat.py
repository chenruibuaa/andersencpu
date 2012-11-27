#!/usr/bin/env python
#
#
from __future__ import with_statement, print_function
import sys
import re

if len(sys.argv) < 2:
  print("%s <regex> <file1> <file2> ..." % sys.argv[0])
  print(" where regex looks like: ")
  print("  report-(?P<benchmark>\w+)-(?P<input>\w+)\.properties.csv ")
  sys.exit(1)

regex = re.compile(sys.argv[1])

# Print CSV files without duplicating headers,
# add columns based on regex
firstFile = True
for file in sys.argv[2:]:
  m = regex.search(file)
  if m is None:
    print "Failure matching %s" % file
    sys.exit(1)

  g = m.groupdict()
  with open(file, 'r') as f:
    first = True
    for line in f:
      line = line.rstrip('\n')
      if first:
        if firstFile:
          print '%s,%s' % (line, ','.join(sorted(g.keys())))
        first = False
        continue
      print '%s,%s' % (line, ','.join([g[k] for k in sorted(g.keys())]))
  firstFile = False
