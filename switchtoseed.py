#!/usr/bin/env python

import glob
import os
debug = True

filestochange = glob.glob('/home/aringler/rewritedata/onemorefix/*/*/*/*.rw')

for curfile in filestochange:
	newfile = curfile.replace('.rw','')
	if debug:
		print(newfile)
		print(curfile)
	os.system('mv ' + curfile + ' ' + newfile)
	if '.512.' in newfile:
		reclen = 512
	else:
		reclen = 4096	
	os.system('./DQseed -Q -b ' + str(reclen) + ' ' + newfile)

