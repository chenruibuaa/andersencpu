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
# File: codeFormatter.py 

from __future__ import print_function
import os
import sys
import getopt
import re


# global constants
GALOIS_COPYRIGHT='COPYRIGHT'
LONESTAR_COPYRIGHT='COPYRIGHT'

# won't enter the EXCLUDE_DIRS
EXCLUDE_DIRS = ['.svn', 'lib', 'input', 'classes']

# these directories get the lonestar copyright
LONESTAR_DIRS = ['apps', 'src/galois/objects', 'src/util' ]

# these are the words we look for in the comment header of a file
# if the comment header does not contain the keyword then we assume that this
# file is owned by someone else
GALOIS_KEYWORD = 'Galois'
LONESTAR_KEYWORD = 'Lonestar'

DEBUG=False


# command line options
FORMAT_OP = 'format'
DEBUG_OPT = 'd'
REMOVE_LIC_OPT='removelic'

class FormatterWrapper(object):

  def __init__(self):
    self.formatterTbl = {}
    self.formatterTbl['java']       = JavaFormatter()      
    self.formatterTbl['xml']        = XmlFormatter()       
    self.formatterTbl['py']         = PythonFormatter()    
    self.formatterTbl['R']          = RFormatter()    
    self.formatterTbl['properties'] = PropertiesFormatter()
    self.formatterTbl['conf']       = ConfFormatter()
    self.formatterTbl['template']   = TemplateFormatter()

  def format(self, fileNames):
    """
    Format the list of names of files passed as arg
    """

    (fileNames, excluded) = self.filterPaths(fileNames, EXCLUDE_DIRS)
    (galoisFiles, lonestarFiles) = self.filterPaths(fileNames, LONESTAR_DIRS)
  
    galoisGroups = self.hashByType(galoisFiles)

    for type,filesList in galoisGroups.items():
      if self.formatterTbl.has_key(type):
        self.formatterTbl[type].format(filesList, GALOIS_COPYRIGHT, GALOIS_KEYWORD)
      else:
        debug('FormatterWrapper.format(): Files of unknown type skipped: %s'%(', '.join(filesList)))

    lonestarGroups = self.hashByType(lonestarFiles)

    for type,filesList in lonestarGroups.items():
      if self.formatterTbl.has_key(type):
        self.formatterTbl[type].format(filesList,LONESTAR_COPYRIGHT, LONESTAR_KEYWORD)
      else:
        debug('FormatterWrapper.format(): Files of unknown type skipped: %s'%(', '.join(filesList)))

  def addCopyRight(self, fileNames):
    """
    Add the copyright message the names of files passed as arg
    """
    (fileNames, excluded) = self.filterPaths(fileNames, EXCLUDE_DIRS)
    (galoisFiles, lonestarFiles) = self.filterPaths(fileNames, LONESTAR_DIRS)
  
    galoisGroups = self.hashByType(galoisFiles)

    for type,filesList in galoisGroups.items():
      if self.formatterTbl.has_key(type):
        self.formatterTbl[type].addCopyRight(filesList)
      else:
        debug('FormatterWrapper.addCopyRight(): Files of unknown type skipped: %s'%(', '.join(filesList)))

    lonestarGroups = self.hashByType(lonestarFiles)

    for type,filesList in lonestarGroups.items():
      if self.formatterTbl.has_key(type):
        self.formatterTbl[type].addCopyRight(filesList, LONESTAR_COPYRIGHT, LONESTAR_KEYWORD)
      else:
        debug('FormatterWrapper.addCopyRight(): Files of unknown type skipped: %s'%(', '.join(filesList)))

  def removeCopyRight(self, fileNames):
    """
    Remove the copyright message the names of files passed as arg
    """

    (fileNames, excluded) = self.filterPaths(fileNames, EXCLUDE_DIRS)
    fileGroups = self.hashByType(fileNames)

    debug('Processing files = %s'%(', '.join(fileNames)))

    for type,filesList in fileGroups.items():
      if self.formatterTbl.has_key(type):
        self.formatterTbl[type].removeCopyRight(filesList)
      else:
        debug('FormatterWrapper.removeCopyRight(): Files of unknown type skipped: %s'%(', '.join(filesList)))
    
  def hashByType(self, fileNames):
    """
    Groups the list of fileNames by their type, where name extension
    serves as the type of the file
    """
    fileGroups = {}

    for (ext,fName) in [ (f.split('.')[-1], f) for f in fileNames ]:
      fileGroups.setdefault(ext, []).append(fName)

    return fileGroups

  def filterPaths(self, fileNames, exclDirs   ):
    """
    inputs:
      fileNames = list of input file path names
      exclDirs = list of directories

    outputs:
      filtered = list of file path names that don't contain an entry from exclDirs
      excluded = list of file path names than contain an entry from exclDirs
    """
    filtered = []
    excluded = []
    for f in fileNames:
      for d in exclDirs:
        if os.path.commonprefix([d, os.path.normpath(f)]) in exclDirs:
          excluded.append(f)
          break

      else: # when for loop doesn't exit with a break
        filtered.append(f)

       
      # (dirPath, x) = os.path.split(f)
      # if os.path.basename(dirPath) in exclDirs:
        # excluded.append(f)
      # else:
        # filtered.append(f)


    return (filtered, excluded)



class Formatter(object):

  def __init__(self):
    self.multiLine = False
    self.commentBegin = '#'
    self.commentEnd = ''


  def addCopyRight(self, fileNames, copyRightFile=GALOIS_COPYRIGHT, copyRightKey = GALOIS_KEYWORD):
    """
    add the copyright message to the files
    """

    cf = open(copyRightFile)
    copyRightLines = cf.readlines()
    copyRightMsg = ''.join(copyRightLines)
    cf.close()

    for f in fileNames:
      debug('Formatter.addCopyRight(): processing  file: %s'%f )
      fh = open(f, 'r+')
      original = fh.read()
      header = self.getHeader(original, copyRightKey)

      # add the file name to copyright message if in LoneStar
      # may enable file names for Galois later on
      fNameMsg = ''
      if copyRightKey == LONESTAR_KEYWORD:
        fNameMsg = 'File: %s \n'%(os.path.basename(f))


      copyRightText = ''
      if self.multiLine:
        merged = ''.join(copyRightLines)
        copyRightText = '\n'.join([self.commentBegin, merged, fNameMsg,  self.commentEnd])
      else:
        copyRightText = ''.join([ '%s %s'%(self.commentBegin,l) for l in copyRightLines ])
        copyRightText += '%s \n%s %s'%(self.commentBegin, self.commentBegin, fNameMsg)


      if header == '':
        # no copyright header, need to add one
        updated = ''
        if original.startswith('#!'):
          # preserve the interpreter line when adding copyright header
          (interp, nl, rest) = original.partition('\n')
          updated = '\n\n'.join([ interp, copyRightText, rest ])
        else:
          updated = '\n\n'.join([ copyRightText, original ])

        fh.seek(0)
        fh.truncate(0)
        fh.write(updated)
        debug('Formatter.addCopyRight(): added copyright message in %s'%f)
      elif copyRightKey in header:
        # is a Galois header and different from (potentially older than) copyRightText, replace it
        if header in original:
          debug('Formatter.addCopyRight(): header in original');

        # print header;
        updated = original.replace(header, copyRightText, 1)
        fh.seek(0)
        fh.truncate(0)
        fh.write(updated)
        debug('Formatter.addCopyRight(): replaced copyright message in %s'%f)
      else:
        # some other header, keep it
        debug('Formatter.addCopyRight(): some other header present, skipped: %s'%f)

      fh.close()


  def format(self, fileNames, copyRightFile=GALOIS_COPYRIGHT, copyRightKey=GALOIS_KEYWORD ):
    """ Format the set of files passed as an argument"""
    self.addCopyRight(fileNames, copyRightFile, copyRightKey) 


  def getHeader(self, alltext, copyRightKey ):
    """
    return the first comment block, expected to be a copyright 
    header
    """

    if self.multiLine:
      # first block of comment ignoring leading white space
      p = re.compile(r'\s*%s.*?%s'%(re.escape(self.commentBegin), re.escape(self.commentEnd)), re.MULTILINE | re.DOTALL)
      m = re.match(p, alltext)
      return m.group(0) if m else ''
    else:
      textArray = []
      seenHeader = False;
      for l in alltext.split('\n'):
        if l.startswith('#!'):
          # handling the shebang line e.g. #!/usr/bin/python
          continue
        elif l.strip() == '':
          if not seenHeader:
            continue
            # empty line before the header msg.
          else:
            break

        elif l.startswith(self.commentBegin):
          if copyRightKey in l:
            seenHeader = True

          textArray.append(l)

        else:
          break

      return '\n'.join(textArray)

  def removeCopyRight(self, fileNames):
    """
    remove the copyright header or any bunch of comments in the beginning
    of the files in the fileNames list
    """

    copyRightKey = GALOIS_KEYWORD
    for f in fileNames:
      debug('Formatter.removeCopyRight(): processing  file: %s'%f )
      fh = open(f, 'r+')
      original = fh.read()
      header = self.getHeader(original, copyRightKey)
     
      updated = original.replace(header, '', 1)
      if header != '' and (GALOIS_KEYWORD in header or LONESTAR_KEYWORD in header):
        debug('Formatter.removeCopyRight(): removing copyright from file: %s'%f)
        fh.seek(0)
        fh.truncate(0)
        fh.write(updated)
      elif header == '':
        debug('Formatter.removeCopyRight(): no header found in the file: %s'%f)
      else:
        debug('Formatter.removeCopyRight(): copyRightKey not found so skipping file: %s'%f)
      
      fh.close()




class JavaFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()
    self.multiLine = True
    self.commentBegin = '/*'
    self.commentEnd = '*/'

  def format(self, files, copyRightFile, copyRightKey):
    if files:
      self.addCopyRight(files, copyRightFile, copyRightKey)
      debug( 'JavaFormatter.format(): formatting java files: %s'%files)
      command = 'ant format -Dformat.java-files="%s" > /dev/null' % (','.join(files))
      debug( 'JavaFormatter.format(): executing: %s'%command)
      os.system(command)
      debug( 'JavaFormatter.format(): done')

class XmlFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()
    self.multiLine = True
    self.commentBegin = '<!--'
    self.commentEnd = '-->'

class PythonFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()

class RFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()

class PropertiesFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()

class ConfFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()

# for java template files
class TemplateFormatter(Formatter):
  def __init__(self):
    super(self.__class__,self).__init__()
    self.multiLine = True
    self.commentBegin = '/*'
    self.commentEnd = '*/'


def usage():
  print('''usage:
# from the top level project director i.e. Galois/trunk
#
python scripts/codeFormatter.py [options] file1 file2 file3 dir1 dir2

options:
  --format 
     will format the files as well. By default only the copyright message is added
     to the files if it is not already there
  -d
    turn on debug messages
  --removelic
    removes the copyright license header from the file
  ''')

def debug(s):
  if DEBUG:
    print(">>> %s" % s)


def main(args):
  if not args or len(args) == 0:
    usage()
  else:
    try:
      opts, args = getopt.getopt(args, '%s'%DEBUG_OPT, [ FORMAT_OP, REMOVE_LIC_OPT ])

    except getopt.GetoptError, err:
      # print help information and exit:
      print(str(err)) # will print something like "option -a not recognized"
      usage()
      sys.exit(2)

    format = False
    removeLic = False

    for o,v in opts:
      if o == '--%s'%FORMAT_OP:
        format = True
      if o == '--%s'%REMOVE_LIC_OPT:
        removeLic = True
      if o == '-%s'%DEBUG_OPT:
        global DEBUG
        DEBUG = True


    dirs = [ x for x in args if os.path.isdir(x) ]
    files = [ x  for x in args if x not in dirs ]
    fw = FormatterWrapper()

    debug('main(): initial files = %s'%files)

    # get all the files in the dirs specified in cmd line args
    for d in dirs:
      if os.path.basename(d) in EXCLUDE_DIRS:
        debug('main(): not formatting the dir "%s" because it is in the exclude lists'%d)
        continue

      for root,subdirs,listings in os.walk(d):
        if os.path.basename(root) in EXCLUDE_DIRS:
          debug('main(): not formatting the dir "%s" because it is in the exclude lists'%root)
          continue

        for sd in subdirs:
          if os.path.basename(sd) in EXCLUDE_DIRS:
            debug('main(): not formatting the dir "%s" because it is in the exclude lists'%sd)
            subdirs.remove(sd)

        for l in listings:
          ext = l.split('.')[-1]
          # debug('main(): Adding to the list %s, root = %s'%(l,root))
          files.append(os.path.join(root, l))


    
    debug('main(): processing files= %s'%(', '.join(files)))
        

    if removeLic:
      fw.removeCopyRight(files)
    elif format:
      fw.format(files)
    else:
      fw.addCopyRight(files)

if __name__ == "__main__":
  main(sys.argv[1:])
