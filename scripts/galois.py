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
from __future__ import print_function
import sys
import os
import re
import subprocess
import optparse
import shlex
import signal


LIBDIR = 'lib'
CLASSDIR = 'classes'
GALOIS_RUNTIME = 'galois.runtime.GaloisRuntime'


def basedir():
  r = os.path.join(os.path.dirname(sys.argv[0]), os.pardir)
  r = os.path.normpath(r)
  return r

class Galois:
  def __init__(self, custom_options=[]):
    self.custom_options = custom_options
    self.set_ld_library_path()
    self._parseArgs()

  def reset(self):
    self._parseArgs()

  def _parseArgs(self):
    # (arg?, parser_option)
    self.config = [
      ('-r', optparse.make_option('-r', dest='runs', action='store', default=1, type="int",
                      help='use RUNS runs in the same vm', metavar='RUNS')),
      ('-t', optparse.make_option('-t', dest='threads', action='store', default=1, type="int",
                      help='use number of threads=THREADS', metavar='THREADS')),
      ('-f', optparse.make_option('-f', dest='prop', action='store',
                      help='read app arguments from properties file', metavar='PROP')),
      ('-g', optparse.make_option('-g', dest='profile', action='store_true', default=False,
                      help='perform profiling of the runtime')),
      ('-s', optparse.make_option('-s', dest='useSerial', action='store_true', default=False,
                      help='use serial runtime')),
      ('-i', optparse.make_option('-i', dest='ignoreFlags', action='store_true', default=False,
                      help='ignore user flags')),
      ('-p', optparse.make_option('-p', dest='useParameter', action='store_true', default=False,
                      help='use parameter')),
      ('', optparse.make_option('--java', dest='java', action='store', default="java",
                      help='use JAVA', metavar='JAVA')),
      ('', optparse.make_option('--vm', dest='vmopts', action='append', default=[],
                      help='pass VMOPT to java', metavar='VMOPT')),
      ('', optparse.make_option('-m', dest='mem', action='store', default='', type='string',
                      help='use HEAPSIZE memory', metavar='HEAPSIZE')),
      ('', optparse.make_option('-d', dest='da', action='store_true', default=False,
                      help='disable assertions')),
      ('', optparse.make_option('--verbose', dest='verbose', action='store_true', default=False,
                      help='enable debug prints'))
    ]
    
    self.config += [('', i) for i in self.custom_options];
    parser = optparse.OptionParser(option_list=[opt for (arg, opt) in self.config], 
        usage='usage: %prog [options] [--] [class] [args]')
    (self.options, self.args) = parser.parse_args()
    if not self.args:
      parser.error('need classname to run')

  def set_ld_library_path(self):
    path = os.path.join(basedir(), LIBDIR)
    if 'LD_LIBRARY_PATH' in os.environ:
      os.environ['LD_LIBRARY_PATH'] = '%s:%s' % (os.environ['LD_LIBRARY_PATH'], path)
    else:
      os.environ['LD_LIBRARY_PATH'] = path

  def classpath(self):
    path = os.path.join(basedir(), LIBDIR)
    files = [os.path.join(path,f) for f in os.listdir(path) if re.search(r'.jar$',f)]
    files.append(os.path.join(basedir(), CLASSDIR))
    return files

  def vmargs(self):
    ea = '-ea'
    if self.options.da:
      ea = '-da'
    # XXX(ddn) 5/25/10. As of Java5/6 -XX:+UseParallelGC is enabled for
    # most machines > 2 CPUs, but parallel GC of oldspace is still
    # not on. Enable both to be sure. Set ratio of new to old
    # (default 2 on "servers") to favor a larger nursery because
    # concurrent Java code generates a lot of short-lived garbage.
    vmargs = ['-XX:+UseParallelGC', '-XX:+UseParallelOldGC', '-XX:NewRatio=1']
    vmargs.append(ea)
    # XXX(ddn) 11/28/10. Use large pages to reduce contention in
    # kernel
    #vmargs.append('-XX:+UseLargePages')
    if self.options.mem:
      vmargs.extend(['-Xms%s' % self.options.mem, '-Xmx%s' % self.options.mem])
    vmargs.extend(self.options.vmopts)
    return vmargs

  def gargs(self):
    r = []
    for (arg, opt) in self.config:
      if not arg:
        continue
      if hasattr(self.options, opt.dest):
        optarg = getattr(self.options, opt.dest)
        if optarg:
          r.append(arg)
          if type(optarg) != type(bool()):
            r.append(str(optarg))
    return r

  def command(self, vmargs=None, classpath=None, gargs=None, args=None):
    if classpath is None:
      classpath = self.classpath()
    if vmargs is None:
      vmargs = self.vmargs()
    if gargs is None:
      gargs = self.gargs()
    if args is None:
      args = self.args
    return '%s %s -cp %s %s %s %s' % (self.options.java,
        ' '.join(vmargs), 
        ':'.join(classpath), GALOIS_RUNTIME, ' '.join(gargs), ' '.join(args))

  def run(self, vmargs=None, classpath=None, gargs=None, args=None):
    """
    params:
      vmargs: list of vmargs 
      classpath: list of Java classpath elements
      gargs: list of args passed to GALOIS_RUNTIME
      args: list of command line arguments containing main class and args
    """
    cmd = self.command(vmargs, classpath, gargs, args)
    retcode = subprocess.call(shlex.split(cmd))
    if retcode != 0:
      # XXX(ddn): Strange behavior where last of subprocess java command is
      # truncated sys.stderr.flush() doesn't fix it but sys.stderr.write("")
      # does...
      sys.stderr.write("")
      sys.stderr.write("Error running command: %s\n" % cmd)
      sys.stderr.flush()
      sys.exit(1)



def main():
  Galois().run()


if __name__ == '__main__':
  signal.signal(signal.SIGQUIT, signal.SIG_IGN)
  main()
