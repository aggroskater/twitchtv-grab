#!/usr/bin/python

import warc
from cStringIO import StringIO #for converting payloads into binary strings
# that reside in-memory rather than on-file
import os #for file length
from subprocess import call #for executing ffmpeg
import shlex #for properly parsing command lines for insertion into call()s
import glob #for grabbing all *.jpg files

print "Compressing snapshots."

imagelist = glob.glob("*.jpg")
imageliststring = ' '.join(imagelist)
tarcommand = "tar -czvf snapshots.tar.gz " + imageliststring

# compress all the snapshots
tarargs = shlex.split(tarcommand)
call(tarargs)

# delete jpgs
rmcommand = "rm " + imageliststring
rmargs = shlex.split(rmcommand)
call(rmargs)
