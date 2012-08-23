#
#
# Copyright  (c) 2011-2012, Hortonworks Inc.  All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
import os, stat
import logging
import time
import subprocess
import platform

def createFile():    
    print "Begin starting processing CreateFile2"
    currentPath = os.path.dirname(os.path.abspath(__file__))
    filePath=os.path.join(currentPath, 'tmp')
    symLinkPath=os.path.join(currentPath, 'mysymlink.txt')
    if not os.path.isdir(filePath):
        os.makedirs(filePath)
    fileName = os.path.join(filePath, 'FileCreatedByJob.log')
    print fileName
    if os.path.isfile(fileName):
        os.unlink(fileName)
    # create new file        
    f = open(fileName, "wb")
    f.seek(99)
    f.write('\0')
    f.close()
    #creating a symlink to the file just created
    if platform.system()=='Windows':
        subprocess.call('mklink ' + symLinkPath + ' ' + fileName, shell=True)
    else:
        linkCmd = 'ln -s ' + fileName + ' ' + symLinkPath
        print "linkCmd: " + linkCmd
        subprocess.call(linkCmd, shell=True)    
    time.sleep(180)
    print "End processing CreateFile2"  

if __name__ == '__main__':
    createFile()
    
    