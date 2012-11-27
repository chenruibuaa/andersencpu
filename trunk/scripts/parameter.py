#!/usr/bin/env python
# Galois, a framework to exploit amorphous data-parallelism in irregular
# programs.
# 
# Copyright (C) 2010, The University of Texas at Austin. All rights reserved.
# UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS SOFTWARE
# AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY, FITNESS FOR ANY
# PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF PERFORMANCE, AND ANY
# WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF DEALING OR USAGE OF TRADE.
# NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH RESPECT TO THE USE OF THE
# SOFTWARE OR DOCUMENTATION. Under no circumstances shall University be liable
# for incidental, special, indirect, direct or consequential damages or loss of
# profits, interruption of business, or related expenses which may arise from use
# of Software or Documentation, including but not limited to those resulting from
# defects in Software and/or Documentation, or loss or inaccuracy of data of any
# kind.
# 
# 

from __future__ import print_function

import sys
import os
import re
import subprocess
import optparse
import shlex
import signal

import galois

STATS = 'stats.txt'
MERGED_STATS = 'parameterStats.csv'
VISUALOUT = 'parameterProfile.pdf'

UNIX_R = 'Rscript'
WIN_R = 'Rscript.exe'
# relative path to R plotting script
R_SCRIPT_PATH = 'scripts/parameter.R'

PATTERN_MERGED_STATS = r'\= Merged Statistics \='
PATTERN_BEGIN_STATS = r'^Active Nodes'
PATTERN_END_STATS = r'\= End Parameter Statistics \='

def die(msg):
  """
  exit with msg
  """
  sys.stderr.write('%s\n'%msg)
  sys.exit(-1)

def get_rpath(rpath=None):
  """
  tries to find if R is present on the path
  or if R_PATH is defined
  """
  if not rpath and os.environ.has_key('R_PATH'):
    rpath = os.environ['R_PATH']
  if rpath is not None and os.access(rpath, os.X_OK):
    return rpath
  for rpath in [UNIX_R, WIN_R]:
    testcmd = '%s --version' % rpath
    try:
      subprocess.check_call(shlex.split(testcmd))
    except subprocess.CalledProcessError:
      continue
    except OSError:
      continue
    else:
      return rpath
  return None

  
def run_r(rpath):
  """
  reads the parameter generated STATS file and generates
  a new file MERGED_STATS to run R on it
  """

  stats = open(STATS, 'r')
  csv = None

  found_merged = False
  found_begin = False
  for l in stats:
    if not found_begin:
      if not found_merged and re.search(PATTERN_MERGED_STATS, l):
        found_merged = True
      if found_merged and re.search(PATTERN_BEGIN_STATS, l):
        found_begin = True
        csv = open(MERGED_STATS, 'w')
        csv.write(l)
    else:
      if re.search(PATTERN_END_STATS,l):
        csv.close()
        break
      else:
        csv.write(l)
  else:
    die('could not find merged parameter stats in %s' % STATS)


  rpath = get_rpath(rpath)
  if rpath:
    rscript = os.path.join(galois.basedir(), R_SCRIPT_PATH)
    rcmd = '%s %s "%s" "%s"' % (rpath, rscript, VISUALOUT, MERGED_STATS)
    subprocess.check_call(shlex.split(rcmd))
  else:
    print('''
    could not find R satistical tool, please do one of the following:
      define environment variable R_PATH
      add R to standard path
      use --help to see how to pass R on cmd line
    ParaMeter text results are in %s
    ''' % MERGED_STATS)



def main():
  custom_options = [optparse.make_option('--rpath', dest='rpath', action='store', default=UNIX_R, 
      type='string', help='provide the path to R statistical and plotting tool')]

  g = galois.Galois(custom_options)
  g.options.useParameter = True
  g.run()
  run_r(g.options.rpath)


if __name__ == '__main__':
  signal.signal(signal.SIGQUIT, signal.SIG_IGN)
  main()
