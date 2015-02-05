#!/usr/bin/env python
import os
import array
import numpy
import struct
import ctypes
import bitstring
import shutil
import sys
import glob
import subprocess

from multiprocessing import Pool

debug = False
seedRWloc = '/home/aringler/rewritedata/onemorefix'
finaldir = '/TEST_ARCHIVE/rewriteQ'

makeTheData = False
fixdata = True
movedata = False


###################################################################################################
#
#Program to correct bad miniseed data
#By: Adam Ringler
#
#fixSampleCnt()
#fixRIC()
#fixBadDecode()
#getBadIRISData()
#
###################################################################################################
 

#Function to fix sample counts we use chkseed to get the correct number and then change the sample
# count by changing the binary bytes
def fixSampleCnt(fileName,recordNumbers,correctNumbers,recLen):

	numberRecords = os.stat(fileName).st_size/recLen

	fileHandle = bitstring.ConstBitStream(filename = fileName)
	newFileHandle = open(fileName + '.rw','wb')

	for recordCount in xrange(0,numberRecords):
		data = fileHandle.read(recLen*8)
		if recordCount + 1 in recordNumbers and len(recordNumbers) > 0:
			if debug:
				print 'Here is the sample count: ' + str(data[30*8:32*8].peek('uint:16'))
			newdata = data[0:30*8] + bitstring.pack('uint:16',correctNumbers[0]) + data[32*8:]
			newdata.tofile(newFileHandle)
			recordNumbers.pop(0)
			correctNumbers.pop(0)
			if debug:
				print 'Here is how many bad records we have left: ' + str(len(recordNumbers))
		else:
			data.tofile(newFileHandle)
	newFileHandle.close()
	if '.rw' in fileName:
		shutil.move(fileName + '.rw',fileName)
	return

#Function to fix reverse integration constants
def fixRIC(fileName,recordNumbers,correctRIC,recLen):
	numberRecords = os.stat(fileName).st_size/recLen
	fileHandle = bitstring.ConstBitStream(filename = fileName)
	newFileHandle = open(fileName + '.rw','wb')
	for recordCount in xrange(0,numberRecords):
		data = fileHandle.read(recLen*8)
		if recordCount + 1 in recordNumbers and len(recordNumbers) > 0:
			dataStart = data[44*8:46*8].peek('uint:16')
			if debug:
				print 'Here is the data start: ' + str(dataStart)
				print 'Here is the RIC location: ' + str(data[(dataStart + 8)*8:(dataStart + 12)*8].peek('int:32'))
			newdata = data[0:(dataStart + 8)*8] + bitstring.pack('int:32',correctRIC[0]) + data[(dataStart + 12)*8:]
			newdata.tofile(newFileHandle)
			recordNumbers.pop(0)
			correctRIC.pop(0)
			if debug:
				print 'Here is how many bad records we have left: ' + str(len(recordNumbers))
		else:
			data.tofile(newFileHandle)
	newFileHandle.close()
	if '.rw' in fileName:
		shutil.move(fileName + '.rw',fileName)
	return

#Function to fix bad decode errors
def fixBadDecode(fileName,recordNumbers,recLen):
	numberRecords = os.stat(fileName).st_size/recLen
	fileHandle = open(fileName,"rb")
	newFileHandle = open(fileName + ".rw",'wb')
	for recordCount in xrange(0,numberRecords):
		data = fileHandle.read(recLen)
		if recordCount + 1 in recordNumbers and len(recordNumbers) > 0:
			if debug:
				print 'Skipping record: ' + str(recordCount + 1)
		else:
			newFileHandle.write(data)
	fileHandle.close()
	newFileHandle.close()
	return

#Function to get the data that IRIS does not like
def getBadIRISData(fileToCheck,year):
	fIRISHandle = open(fileToCheck)
	IRISList = fIRISHandle.readlines()
	fIRISHandle.close()
	IRISList = set(IRISList)
	dirList = []
	for item in IRISList:
		try:
			if int(item.split(".")[2]) == year:
				dirList.append('/xs0/seed/' + item.split(".")[1] + '_' + item.split(".")[0] + \
					'/' + item.split(".")[2] + '/' + item.split(".")[2] + "_" + \
					item.split(".")[3].strip() + "_" + item.split(".")[1] + "_" + item.split(".")[0])
		except: 
			print item
	return dirList

#Function to get Jerry's list of bad data
def getBadData(dirToCheck):
	dataFiles = glob.glob(dirToCheck)
	allSeedDir = []
	#Loop through all of Jerry's suggested issues
	for dataFile in dataFiles:
		#Open up one of the data files and get the day of the problem
		fHandle = open(dataFile,'r')
		for line in fHandle:
			if 'seed' in line:
				seedFile = ' '.join(line.split())
				seedFile = (seedFile.split(' '))[2]
				seedDir = seedFile.rsplit('/',1)[0]
				allSeedDir.append(seedDir)
		fHandle.close()
	allSeedDir = list(set(allSeedDir))
	return allSeedDir


#Function to move the data to a new location and begin writing it
def makeData(seedDir):
	if debug:
		print 'Moving ' + seedDir
	newDir = seedDir.split('/')
	if not os.path.exists(seedRWloc + '/' + newDir[3]):
		os.mkdir(seedRWloc + '/' + newDir[3])
	if not os.path.exists(seedRWloc + '/' + newDir[3] + '/' + newDir[4]):
		os.mkdir(seedRWloc + '/' + newDir[3] + '/' + newDir[4])
	if not os.path.exists(seedRWloc + '/' + newDir[3] + '/' + newDir[4] + '/' + newDir[5]):
		os.mkdir(seedRWloc + '/' + newDir[3] + '/' + newDir[4] + '/' + newDir[5])
	seedfiles = glob.glob(seedDir + '/*.seed')
	for seedfile in seedfiles:
		shutil.copy2(seedfile,seedRWloc + '/' + newDir[3] + '/' + newDir[4] + '/' + newDir[5])
	return

#Here we actually fix the data
def reScanDataBadDecode(filetoRW):
	try:
		if debug:
			print filetoRW
		reclen = int(filetoRW.split(".")[1])
	except:
		reclen = 4096	
	realdata = False
	proc = subprocess.Popen(['/dcc/bin/dumpseed',filetoRW], stdout = subprocess.PIPE, \
		stderr = subprocess.PIPE)
	

	if debug:
		print 'Working on: ' + filetoRW
		
	
	for line in proc.stdout:
		if debug:
			print line
		if "1000-DATAONLY: STEIM" in line:
			reclen = line.split(" ")[3]
			reclen = 2**int(reclen.replace("Len=",""))
			realdata = True
			if debug:
				print 'We have real data'
		elif "1000-" in line and "STEIM" not in line:
			try:
				reclen = (line.split(" ")[3]).rstrip()	
				reclen = reclen.replace("Len=","")
				reclen = 2**int(reclen)
				realdata = False
			except:
				print 'We got a bad one moving on'
		
	
	if debug:
		print 'Here is the record length: ' + str(reclen)
		print 'Do we have real data: ' + str(realdata)
	
	stdout, stderr = proc.communicate()
	proc.stdout.close()
	proc.stderr.close()
	proc.wait()

	#Now we get the errors
	proc = subprocess.Popen(['./chkseed','-R ' + str(reclen),'-v','-i' + filetoRW], \
		stdout=subprocess.PIPE, stderr=subprocess.PIPE) 

	#Lets now find what kind of errors we have
	errortype = []
	record =[]
	corrValue=[]
	rewrotefile = False
	for line in proc.stdout:
		if debug:
			print line
		if "Error" in line:
			rewrotefile = True
			if debug:
				print "Here is the line: " + line.rstrip()
			if "Error" in line:
				line = ' '.join(line.split())
				if "Bad decode" in line:
					errortype.append('BadDecode')
					recNum = line.split(' ')[-1]
					recNum = int(recNum.replace("(","").replace(")",""))
					record.append(recNum)
				
			if debug:
				print "Here is the record: " + str(record)
				print "Here is the errortype: " + str(errortype)
	
	if len(errortype) > 0:
		fixBadDecode(filetoRW,record,reclen)
	proc.stdout.close()
	proc.stderr.close()
	proc.wait()
			


	return

#Here we actually fix the data
def reScanDataSampleCnt(filetoRW):
	try:
		if debug:
			print filetoRW
		reclen = int(filetoRW.split(".")[1])
	except:
		reclen = 4096	
	realdata = False
	proc = subprocess.Popen(['/dcc/bin/dumpseed',filetoRW], stdout = subprocess.PIPE, \
		stderr = subprocess.PIPE)
	if debug:
		print 'Working on: ' + filetoRW
	for line in proc.stdout:
		if "1000-DATAONLY: STEIM" in line:
			reclen = line.split(" ")[3]
			reclen = 2**int(reclen.replace("Len=",""))
			realdata = True
		elif "1000-" in line and "STEIM" not in line:
			try:
				reclen = (line.split(" ")[3]).rstrip()	
				reclen = reclen.replace("Len=","")
				reclen = 2**int(reclen)
				realdata = False
			except:
				print 'We got a bad one moving on'
	if debug:
		print 'Here is the record length: ' + str(reclen)
		print 'Do we have real data: ' + str(realdata)
	proc.stdout.close()
	proc.stderr.close()
	proc.wait()
	
	#Now we get the errors
	proc = subprocess.Popen(['./chkseed','-R ' + str(reclen),'-v','-i' + filetoRW], \
		stdout=subprocess.PIPE, stderr=subprocess.PIPE) 

	#Lets now find what kind of errors we have
	errortype = []
	record =[]
	corrValue=[]
	rewrotefile = False
	for line in proc.stdout:
		if "Error" in line:
			rewrotefile = True
			if debug:
				print "Here is the line: " + line.rstrip()
			if "Error" in line:
				line = ' '.join(line.split())
				if "Sample count" in line:
					errortype.append('SampleCnt')
					corrValue.append(int(line.split(' ')[7].replace("'","").replace(",","")))
					recNum = line.split(' ')[-1]
					recNum = int(recNum.replace("(","").replace(")",""))
					record.append(recNum)
				
			if debug:
				print "Here is the record: " + str(record)
				print "Here is the errortype: " + str(errortype)
				print "Here is the correct value: " + str(corrValue)
	for error in errortype:
		fixSampleCnt(filetoRW,record,corrValue,reclen)
	proc.stdout.close()
	proc.stderr.close()
	proc.wait()
		
	return



def reScanDataFixRIC(filetoRW):
	try:
		if debug:
			print filetoRW
		reclen = int(filetoRW.split(".")[1])
	except:
		reclen = 4096	
	realdata = False
	try:

		proc = subprocess.Popen(['/dcc/bin/dumpseed',filetoRW], stdout = subprocess.PIPE, \
			stderr = subprocess.PIPE)
		if debug:
			print 'Working on: ' + filetoRW
		for line in proc.stdout:
			if "1000-DATAONLY: STEIM" in line:
				reclen = line.split(" ")[3]
				reclen = 2**int(reclen.replace("Len=",""))
				realdata = True
			elif "1000-" in line and "STEIM" not in line:
				try:
					reclen = (line.split(" ")[3]).rstrip()	
					reclen = reclen.replace("Len=","")
					reclen = 2**int(reclen)
					realdata = False
				except:
					print 'We got a bad one moving on'
		if debug:
			print 'Here is the record length: ' + str(reclen)
			print 'Do we have real data: ' + str(realdata)
	
		proc.stdout.close()
		proc.stderr.close()
		proc.wait()
		
		#Now we get the errors
		proc = subprocess.Popen(['./chkseed','-R ' + str(reclen),'-v','-i' + filetoRW], \
			stdout=subprocess.PIPE, stderr=subprocess.PIPE) 

		#Lets now find what kind of errors we have
		errortype = []
		record =[]
		corrValue=[]
		rewrotefile = False
		for line in proc.stdout:
			if "Error" in line:
				rewrotefile = True
				if debug:
					print "Here is the line: " + line.rstrip()
				if "Error" in line:
					line = ' '.join(line.split())
					if "Bad RIC" in line:
						errortype.append('BadRic')
						corrValue.append(int(line.split(' ')[6].replace("'","").replace(",","").replace(")","")))
						recNum = line.split(' ')[-1]
						recNum = int(recNum.replace("'","").replace(")","").replace("(",""))
						record.append(recNum)
				
				if debug:
					print "Here is the record: " + str(record)
					print "Here is the errortype: " + str(errortype)
					print "Here is the correct value: " + str(corrValue)
		for error in errortype:
			fixRIC(filetoRW,record,corrValue,reclen)
		if len(errortype) == 0 and '.rw' not in filetoRW and not os.path.isfile(filetoRW + '.rw'):
			shutil.move(filetoRW,filetoRW + '.rw')
		proc.stdout.close()
		proc.stderr.close()
		proc.wait()
		
	except:
		print 'Bad file: ' + filetoRW
			
	return

def switchToQ(fileName):

	reclen = 512
	os.system('./DQseed -Q -b ' + str(reclen) + ' ' + fileName)
	

	return





if __name__ == "__main__":


	yearCur = 2011
	pool = Pool(18)
	if makeTheData:
		IRISList = []
		jerryList = []	
		for year in range(yearCur,2006):
			checkDir = '/r02/scans/integrity/*' + str(year) + '*'
			curYearList = getBadData(checkDir)
			if len(curYearList) > 0:
				for item in curYearList:
					jerryList.append(item)
		IRISList = getBadIRISData("IRISreport",year)
		finalList = list(set(jerryList + IRISList))
		pool.map(makeData,finalList)



	if fixdata:
		for year in range(yearCur,yearCur + 1):
			print 'We are on year: ' + str(year)
			for dayran in range(0,4):		
				print 'We are on day range: ' + str(dayran)
				filestoRW = glob.glob(seedRWloc + '/*/'+ str(year) + '/*_' + str(dayran) + '*/*.seed')
				filestoRWgood = []
				for curfile in filestoRW:
					filestoRWgood.append(curfile)
				print 'Here are the number of files to check for bad decode for day range ' + str(dayran) + ': ' + str(len(filestoRWgood))
				if debug:
					print(filestoRWgood)
				pool.map(reScanDataBadDecode,filestoRWgood)
				
				filestoRW = glob.glob(seedRWloc + '/*/'+ str(year) + '/*_' + str(dayran) + '*/*.seed*')
				filestoRWgood = []
				for curfile in filestoRW:
					filestoRWgood.append(curfile)
				print 'Here are the number of files to check for sample count for day range ' + str(dayran) + ': ' + str(len(filestoRWgood))
				if debug:
					print(filestoRWgood)
				pool.map(reScanDataSampleCnt,filestoRWgood)
				
				filestoRW = glob.glob(seedRWloc + '/*/'+ str(year) + '/*_' + str(dayran) + '*/*.seed*')
				filestoRWgood = []
				for curfile in filestoRW:
					filestoRWgood.append(curfile)
				print 'Here are the number of files to check for RIC for day range ' + str(dayran) + ': ' + str(len(filestoRWgood))
				if debug:
					print(filestoRWgood)
				pool.map(reScanDataFixRIC,filestoRWgood)
			
				filestoRW = glob.glob(seedRWloc + '/*/'+ str(year) + '/*_' + str(dayran) + '*/*.seed.rw')
				filestoRWgood = []
				for curfile in filestoRW:
					filestoRWgood.append(curfile)
				print 'Here are the number of files to check for sample count for day range ' + str(dayran) + ': ' + str(len(filestoRWgood))
				if debug:
					print(filestoRWgood)
				pool.map(reScanDataSampleCnt,filestoRWgood)
							
				filestoRW = glob.glob(seedRWloc + '/*/'+ str(year) + '/*_' + str(dayran) + '*/*.seed.rw')
				filestoRWgood = []
				for curfile in filestoRW:
					filestoRWgood.append(curfile)
				print 'Here are the number of files to check for RIC for day range ' + str(dayran) + ': ' + str(len(filestoRWgood))
				if debug:
					print(filestoRWgood)
				pool.map(reScanDataFixRIC,filestoRWgood)



#			for curfile in filestoRWgood:
#				if debug:
#					print 'Here is the current file: ' + curfile
#				reScanDataFixRIC(curfile)
#				reScanDataSampleCnt(curfile)
#				reScanDataBadDecode(curfile)
#				switchToQ(curfile)			
#			

#			pool.map(reScanDataFixRIC,filestoRWgood)

#			pool.map(switchToQ,filestoRWgood)
#			print 'Now on the rescandata'
	
		







