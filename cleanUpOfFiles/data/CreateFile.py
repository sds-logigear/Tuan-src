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
import sys
import time 
def createFile():    
    print "Begin starting processing CreateFile"
    currentPath = os.path.dirname(os.path.abspath(__file__))
    fileName = os.path.join(currentPath, 'FileCreatedByJob.log')
    print fileName
    if os.path.isfile(fileName):
        os.unlink(fileName)    
    # create new file        
    f = open(fileName, "wb")
    f.seek(99)
    f.write("\0")
    f.close()        
    os.chmod(fileName, 00777)  
    print "End processing CreateFile"
    time.sleep(180)    
if __name__ == '__main__':
    createFile()
    
    
