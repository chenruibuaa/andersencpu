#!/usr/bin/env python
#
# Run a series of performance tests and pretty print and export csv
# the results.

from __future__ import with_statement, print_function
import sys
import os
import re
import subprocess
import signal
import galois
import optparse 

H_COMMITTED_ITS = 'committed its'
H_TOTAL_ITS = 'total its'
H_ABORT_RATIO = 'ABORT RATIO'
H_TIME = 'walltime'
H_WOGC_TIME = 'walltime wo gc'
H_THREAD_TIME = 'thread time'
H_IDLE_THREAD_TIME = 'idle time'

H_THREAD = 'T'
H_RUN = 'r'
H_WOGCRUN = 'wogcr'
H_AVERAGE_TIME = 'AVERAGE TIME'
H_LAST_TIME = 'LAST TIME'
H_IDLE_THREAD_TIME = 'IDLE THREAD TIME'
H_GC_TIME = 'GC TIME'
H_SERIAL_TIME = 'SERIAL TIME'
H_SCALABILITY = 'SCALABILITY'
H_SPEEDUP = 'SPEEDUP'
H_CUSTOM = 'CUSTOM'

def genColumns():
  """Generate a map between column headings and their order in tabular output."""
  r = []
  r.append(H_THREAD)
  for idx in range(1, G.options.runs + 1):
    r.append('%s%d' % (H_RUN, idx))
  for idx in range(1, G.options.runs + 1):
    r.append('%s%d' % (H_WOGCRUN, idx))
  r.extend([H_LAST_TIME, H_AVERAGE_TIME])
  r.extend([H_COMMITTED_ITS, H_TOTAL_ITS, H_ABORT_RATIO, H_THREAD_TIME])
  r.extend([H_IDLE_THREAD_TIME, H_GC_TIME, H_SERIAL_TIME])
  
  r.extend([H_SCALABILITY])
  r.extend([H_SPEEDUP])
  for idx in range(len(G.options.customCol)):
    r.append('%s%d' % (H_CUSTOM, idx))

  return r


class Table:
  def __init__(self):
    # Representation is a list of dicts
    self.data = []
    self.crow = {}

  def add(self, row):
    self.crow.update(row)

  def next(self):
    self.data.append(self.crow)
    self.crow = {}

  def lines(self):
    lines = []
    lines.append(','.join(COLUMNS))
    for row in self.data:
      line = []
      for col in COLUMNS:
        if col in row:
          line.append(str(row[col]))
        else:
          line.append('')
      lines.append(','.join(line))
    return lines


def printBright(s):
  red = '\033[1;31m'
  endc = '\033[0m'
  print(red + s + endc)
  sys.stdout.flush()


def die(s):
  sys.stderr.write(s)
  sys.exit(1)


def numify(list):
  try:
    return [float(x) for x in list]
  except TypeError:
    return float(list)


def mean(list):
  return sum(list) / len(list)


def drop1mean(list):
  if len(list) > 1:
    drop1 = list[1:]
    return sum(drop1) / len(drop1)
  return mean(list)


def readStats(serial=False):
  """Read runtime stats left behind by galois execution."""
  # name, (scalar, pat)
  d = {H_COMMITTED_ITS:   (True, True,  re.compile('^Committed Iterations: (\S+)')),
       H_TOTAL_ITS:       (True, True,  re.compile('^Total Iterations: (\S+)')),
       H_ABORT_RATIO:     (True, True,  re.compile('^Abort ratio: (\S+)')),
       H_TIME:            (True, False, re.compile('^With GC \(ms\): \[(.*?)\]')),
       H_WOGC_TIME:       (True, False, re.compile('^Without GC \(ms\): \[(.*?)\]')),
      }

  for idx in range(len(G.options.customCol)):
    d.update({'%s%d' % (H_CUSTOM, idx): (False, True, re.compile(G.options.customCol[idx]))})

  if not serial:
    d.update({H_THREAD_TIME: (True, False, re.compile('^Thread time per measured period \(thread\*ms\): \[(.*?)\]')),
              H_IDLE_THREAD_TIME: (True, False, re.compile('^Idle thread time per measured period \(thread\*ms\): \[(.*?)\]')),
             })

  r = {}
  looking = True
  with open('stats.txt') as f:
    for line in f:
      if looking and line.find('= Merged Statistics =') >= 0:
        looking = False
      if looking:
        continue

      for (name, (req, scalar, pat)) in d.items():
        if not pat:
          continue
        m = pat.match(line)
        if not m:
          continue
        value = m.group(1)
        if scalar:
          r[name] = float(value)
        else:
          r[name] = numify(value.split(','))
        # Find only the first match
        d[name] = (req, False, None)

  for (name, (req, scalar, pat)) in d.items():
    if pat and req:
      die('missing value from stats.txt: %s\n' % name)

  return r


def run(table, t=1, serial=False, sample=False):
  """Runs app, records results in table and returns mean runtime."""
  G.reset()
  G.options.threads = t

  name = t
  if serial:
    name = 'serial'
    G.options.useSerial = True
  if sample:
    name = 'g%d' % t
    G.options.profile = True

  printBright('$ %s' % G.command())
  G.run()
  stats = readStats(serial)

  table.add({H_THREAD: name, H_COMMITTED_ITS: stats[H_COMMITTED_ITS], H_TOTAL_ITS: stats[H_TOTAL_ITS]})
  table.add({H_ABORT_RATIO: stats[H_ABORT_RATIO]})

  headers = ['%s%d' % (H_RUN, i) for i in range(1, G.options.runs + 1)]
  for (h,r) in zip(headers, stats[H_TIME]):
    table.add({h: r})

  headers = ['%s%d' % (H_WOGCRUN, i) for i in range(1, G.options.runs + 1)]
  for (h,r) in zip(headers, stats[H_WOGC_TIME]):
    table.add({h: r})

  lastWalltime = stats[H_TIME][-1]
  table.add({H_AVERAGE_TIME: drop1mean(stats[H_TIME]), H_LAST_TIME: lastWalltime})

  if not (serial):
    lastGctime = stats[H_WOGC_TIME][-1]
    #gctime = drop1mean(stats[H_WOGC_TIME])
    table.add({H_GC_TIME: lastWalltime - lastGctime})

    threadTime = sum(stats[H_THREAD_TIME])
    parallelTime = threadTime / t
    table.add({H_THREAD_TIME: threadTime})
    table.add({H_SERIAL_TIME: lastWalltime - parallelTime})

    idleThreadTime = sum(stats[H_IDLE_THREAD_TIME])
    table.add({H_IDLE_THREAD_TIME: idleThreadTime})

  for idx in range(len(G.options.customCol)):
    col = '%s%d' % (H_CUSTOM, idx)
    if col in stats:
      table.add({col: stats[col]})
    
  return lastWalltime


# Parses thread range from G.options
# Grammar:
#  R := R,R
#     | N
#     | N:N
#     | N:N:N
#  N := an integer
def get_thread_range(num_threads):
  s = G.options.threadRange
  if not s:
    return range(0, num_threads + 1)
  # Parsing strategy: greedily parse integers with one character
  # lookahead to figure out exact category
  s = s + ' ' # append special end marker
  retval = []
  curnum = -1
  curseq = []
  for i in range(len(s)):
    if s[i].isdigit() and curnum < 0:
      curnum = i
    elif s[i].isdigit():
      pass
    elif s[i] == ',' or s[i] == ' ':
      if curnum < 0:
        break
      num = int(s[curnum:i])
      if len(curseq) == 0:
        retval.append(num)
      elif len(curseq) == 1:
        retval.extend(range(curseq[0], num + 1))
      elif len(curseq) == 2:
        retval.extend(range(curseq[0], curseq[1] + 1, num))
      else:
        break
      curnum = -1
      curseq = []
    elif s[i] == ':' and curnum >= 0:
      curseq.append(int(s[curnum:i]))
      curnum = -1
    else:
      break
  else:
    return sorted(set(retval))
  die('error parsing range: %s\n' % s)


def main():
  # NB(ddn): read threads here because run() will modify 
  thread_range = get_thread_range(G.options.threads)
  table = TABLE
  serial = 1
  t1 = 1

  for t in thread_range:
    if t == 0:
      serial = run(table, serial=True)
      table.next()
    elif t == 1:
      t1 = run(table, t=t)
      table.add({H_SCALABILITY: 1, H_SPEEDUP: serial / t1})
      table.next()
    else:
      tn = run(table, t=t)
      table.add({H_SCALABILITY: t1 / tn, H_SPEEDUP: serial / tn})
      table.next()

  if G.options.enableExtra:
    run(table, t=1, sample=True)
    table.next()
    os.rename('stats.txt', 'report-stats-g1.txt')
    max_threads = thread_range[-1]
    if max_threads != 1:
      run(table, t=max_threads, sample=True)
      table.next()
      os.rename('stats.txt', 'report-stats-g%d.txt' % max_threads)

  writeFile()


def writeFile():
  with open('report.csv', 'w') as f:
    f.write('\n'.join(TABLE.lines()))
    f.write('\n')


if __name__ == '__main__':
  signal.signal(signal.SIGQUIT, signal.SIG_IGN)
  custom_options = [
      optparse.make_option('-a', dest='threadRange', default=None,
                           help='run range of threads, default is 0:numThreads. ' +
                           'Where 0 is the serial execution. Accepts start:end; start:end:step; a,b,c;'),
      optparse.make_option('-x', dest='enableExtra', action='store_true', default=False,
                           help='enable sampler results'),
      optparse.make_option('-c', dest='customCol', action='append', default=[], metavar='REGEX',
                           help='use REGEX with grouping to define custom field')
  ]
  G = galois.Galois(custom_options)
  COLUMNS = genColumns()
  TABLE = Table()
  try:
    main()
  except KeyboardInterrupt:
    print('Caught keyboard interrupt, writing result so far...')
    writeFile()
  except subprocess.CalledProcessError:
    print('Caught subprocess error, writing result so far...')
    writeFile()
    sys.exit(1)
