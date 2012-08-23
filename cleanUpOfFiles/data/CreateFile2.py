#
#
# Copyright (c) 2011-2012, Hortonworks Inc. All rights reserved.
#
#
# Except as expressly permitted in a written Agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution or other exploitation of all or any part of the contents
# of this file is strictly prohibited.
#
#
import platform
import subprocess
import time

f = open("c:\\FileCreatedByJob.log","w")
f.write("")
f.close()
fileName = 'c:\\FileCreatedByJob.log'
symLinkPath = "mysymlink.txt"
if platform.system()=='Windows':
    subprocess.call('mklink ' + symLinkPath + ' ' + fileName, shell=True)
elif platform.system()=='Linux':
    subprocess.call('touch /tmp/FileCreatedByJob.log', shell=True)
    subprocess.call('ln -s /tmp/FileCreatedByJob.log mysymlink.txt > /dev/null 2>&1', shell=True)
time.sleep(180)