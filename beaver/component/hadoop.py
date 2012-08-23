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
import StringIO
import os, re, string, time
from beaver.machine import Machine
from beaver.config import Config
from beaver import util

CWD = os.path.dirname(os.path.realpath(__file__))

class Hadoop:
    @classmethod
    def runas(cls, user, cmd, logoutput=True):
        hadoop_cmd = Config.get('hadoop', 'HADOOP_CMD')
        if Config.get('hadoop', 'HADOOP_CONF_EXCLUDE') == 'False':
            hadoop_cmd += " --config " + Config.get('hadoop', 'HADOOP_CONF')
        hadoop_cmd += " " + cmd
        return Machine.runas(user, hadoop_cmd, logoutput=logoutput)

    @classmethod
    def runInBackgroundAs(cls, user, cmd):
        hadoop_cmd = Config.get('hadoop', 'HADOOP_CMD')
        if Config.get('hadoop', 'HADOOP_CONF_EXCLUDE') == 'False':
            hadoop_cmd += " --config " + Config.get('hadoop', 'HADOOP_CONF')
        hadoop_cmd += " " + cmd
        return Machine.runinbackgroundAs(user, hadoop_cmd)

    @classmethod
    def run(cls, cmd, logoutput=True):
        return cls.runas(None, cmd, logoutput)

    @classmethod
    def runInBackground(cls, cmd):
        return cls.runInBackgroundAs(None, cmd)

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "core-site.xml"), propertyValue, defaultValue=defaultValue)

    @classmethod
    def getFSDefaultValue(cls):
        return cls.getConfigValue("fs.default.name")

    @classmethod
    def getTasktrackers(cls):
        slaveFile = os.path.join(Config.get('hadoop', 'HADOOP_CONF'),"slaves")
        f = open(slaveFile, "r")
        tasktrackers = f.readlines()
        return tasktrackers

    @classmethod
    def getDatanodes(cls):
        slaveFile = os.path.join(Config.get('hadoop', 'HADOOP_CONF'),"slaves")
        f = open(slaveFile, "r")
        datanodes = f.readlines()
        return datanodes

    @classmethod
    def resetNamenode(cls,action):
        namenode = HDFS.getNamenode().split()
        Machine.resetNode("namenode", namenode, action)

    @classmethod
    def resetJobtracker(cls,action):
        jobtracker = MAPRED.getJobTracker().split()
        Machine.resetNode("jobtracker", jobtracker, action)

    @classmethod
    def resetTasktracker(cls,action):
        tasktrackers = cls.getTasktrackers()
        Machine.resetNode("tasktracker", tasktrackers, action)

    @classmethod
    def resetDatanode(cls,action):
        datanodes = cls.getDatanodes()
        Machine.resetNode("datanode", datanodes, action)    

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("version")
        if exit_code == 0:
            import re
            pattern = re.compile("^Hadoop (\S+)")
            m = pattern.search(output)
            if m:
                return m.group(1)
        return ""

class HDFS:
    @classmethod
    def fileExists(cls, filepath, user=None):
        exit_code, stdout = Hadoop.runas(user, "dfs -ls " + filepath, logoutput=False)
        return exit_code == 0

    @classmethod
    def createDirectory(cls, directory, user=None, force=False):
        out = [0, '']
        if not cls.fileExists(directory, user):
            out = Hadoop.runas(user, "dfs -mkdir " + directory)
        elif force:
            Hadoop.runas(user, "dfs -rmr -skipTrash " + directory)
            out = Hadoop.runas(user, "dfs -mkdir " + directory)
            if out[0] == 0: out = (0, "Created directory \"%s\"" % directory)
        return out

    @classmethod
    def deleteDirectory(cls, directory, user=None, skipTrash=True, trashProp = ''):
        out = [0, '']
        if trashProp != '':
            delcmd = "dfs " + trashProp + " -rmr "
        else:
            delcmd = "dfs -rmr "
        if skipTrash:
            delcmd = delcmd + "-skipTrash "
            
        delcmd = delcmd + directory
        if cls.fileExists(directory, user):
            out = Hadoop.runas(user, delcmd)
        return out

    @classmethod
    def createFile(cls, filename, user=None, force=False):
        out = [0, '']
        if not cls.fileExists(filename, user):
            out = Hadoop.runas(user, " dfs -touchz " + filename)
        elif force:
            Hadoop.runas(user, "dfs -rm -skipTrash " + filename)
            out = Hadoop.runas(user, "dfs -touchz " + filename)
            if out[0] == 0: out = (0, "Created file \"%s\"" % filename)
        return out

    @classmethod
    def deleteFile(cls, filePath, user=None, skipTrash=True, trashProp = ''):
        out = [0, '']
        if trashProp != '':
            delcmd = "dfs " + trashProp + " -rm "
        else:
            delcmd = "dfs -rm "
        if skipTrash:
            delcmd = delcmd + "-skipTrash "

            
        delcmd = delcmd + filePath
        if cls.fileExists(filePath, user):
            out = Hadoop.runas(user, delcmd)
        return out

    @classmethod
    def createDirectoryAsUser(cls, directory, adminUser=None, user=None, perm="711", force=False):
        out = cls.createDirectory(directory, user=adminUser, force=force)
        if out[1] != "":
            out = Hadoop.runas(adminUser, "dfs -chmod %s %s" % (perm, directory))
            out = Hadoop.runas(adminUser, "dfs -chown -R %s:%s %s" % (user, user, directory))
        return out

    @classmethod
    def copyFromLocal(cls, localpath, hdfspath, user=None):
        return Hadoop.runas(user, "dfs -copyFromLocal %s %s" % (localpath, hdfspath))

    @classmethod   
    def cat(cls, hdfspath, user=None):
        return Hadoop.runas(user, "dfs -cat %s" % hdfspath, logoutput=False)

    @classmethod
    def getDatanodeCount(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -report")
        if exit_code == 0:
            m = re.match(".*Datanodes available: (\d+) \(", output, re.DOTALL)
            if m:
                return int(m.group(1))
        return 0

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "hdfs-site.xml"), propertyValue, defaultValue=defaultValue)

    @classmethod
    def getNamenode(cls):
        return cls.getConfigValue("dfs.http.address")

    @classmethod
    def getNamenodeHost(cls):
        namenode = cls.getNamenode()
        if namenode.find(":") != -1:
            namenode = namenode.split(":")[0]
        return namenode

    @classmethod
    def getReplication(cls):
        return cls.getConfigValue("dfs.replication", defaultValue="3")

    @classmethod
    def isWebhdfsEnabled(cls):
        return cls.getConfigValue("dfs.webhdfs.enabled", defaultValue="true") == "true"

    @classmethod
    def getAccessTimePrecision(cls):
        return cls.getConfigValue("dfs.access.time.precision", defaultValue="3600000")

    @classmethod
    def enterSafemode(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -safemode enter")
        if re.search(".*Safe mode is ON",output) != None:
            return True
        else:
            return False

    @classmethod
    def exitSafemode(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -safemode leave")
        if re.search(".*Safe mode is OFF",output) != None:
            return True
        else:
            return False        
   
    @classmethod
    def waitForNNOutOfSafemode(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -safemode get")
        while (re.search(".*Safe mode is ON",output) != None):
            exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -safemode get")
            time.sleep(20)
        return True

    @classmethod
    def runAdminReport(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -report")
        return output

    @classmethod
    def refreshDatanodes(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "dfsadmin -refreshNodes")
        return output

    @classmethod
    def decompressedText(cls, inpath, outfile):
        return Hadoop.run("jar %s com.hw.util.DecodeFile %s %s" % (os.path.join(CWD, 'HDFSUtil.jar'), inpath, outfile), logoutput=False)

class MAPRED:
    @classmethod
    def triggerSleepJob(cls,numOfMaps,numOfReduce,mapsleeptime,reducesleeptime,numOfJobs,queue='',background=False):
        jobCounter = 0
        while (jobCounter < numOfJobs):
            sleepCmd = " jar " + Config.get('hadoop', 'HADOOP_EXAMPLES_JAR') + " sleep " + queue + " -m " + numOfMaps + " -r " + numOfReduce + " -mt " + mapsleeptime +" -rt " + reducesleeptime
            if background:
                Hadoop.runInBackground(sleepCmd)
            else:
                Hadoop.run(sleepCmd)                
            jobCounter = jobCounter + 1
            
    @classmethod
    def getJobID(cls):
        output = Hadoop.run(" job -list ")
        actLines = output[1].split("\n")
        jobID = ((actLines)[-1]).split()[0]
        return jobID

    @classmethod
    def killAJob(cls,jobID):
        return Hadoop.run(" job -kill %s  " % jobID)

    @classmethod
    def checkForJobCompletion(cls,jobID):
        output =  Hadoop.run(" job -status %s  " % jobID)
        if string.find(output[1],"reduce() completion: 1.0",) == -1:
            return False
        else:
            return True
        
    @classmethod
    def checkForNewAttemptIds(cls,jobID,task="map"):
        output =  Hadoop.run("job -list-attempt-ids " + jobID + " " + task + " running ")
        ##Dont split if output is blank
        if output[1] == "":
            return ""
        return output[1].split("\n")
    
    @classmethod
    def failAttempts(cls,attemptIDs):
        for attemptID in attemptIDs:
            Hadoop.run(" job -fail-task  " + attemptID)

    @classmethod
    def killAttempts(cls,attemptIDs):
        for attemptID in attemptIDs:
            Hadoop.run(" job -kill-task  " + attemptID)

    @classmethod
    def isJobFailed(cls,jobID):
        output =  Hadoop.run(" job -status %s  " % jobID)
        if string.find(output[1],"Failed",) == -1:
            return False
        else:
            return True

    @classmethod
    def getConfigValue(cls, propertyValue, defaultValue=None):
        return util.getPropertyValueFromConfigXMLFile(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"), propertyValue, defaultValue=defaultValue)

    @classmethod
    def getJobTracker(cls):
        return cls.getConfigValue("mapred.job.tracker")

    @classmethod
    def getJobTrackerHttpAddress(cls):
        return cls.getConfigValue("mapred.job.tracker.http.address")

    @classmethod
    def isJobExists(cls,jobID):
        output =  Hadoop.run(" job -list")
        if string.find(output[1],jobID,) == -1:
            return False
        else:
            return True
        
    @classmethod
    def getJobOwner(cls,jobID):
        output = Hadoop.run(" job -list ")
        actLines = output[1].split("\n")
        jobOwner = ((actLines)[-1]).split()[3]
        return jobOwner
        
    @classmethod
    def getTTHostForAttemptId(cls,attemptID):
        HADOOP_JOBTRACKER_LOG = Config.get('hadoop', 'HADOOP_JOBTRACKER_LOG')
        f = open(HADOOP_JOBTRACKER_LOG,"r")
        for line in f:
            searchFor = re.search(".*" + attemptID + ".*tracker_.*/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*",line)
            if searchFor != None:
                return searchFor.group(1)

    @classmethod
    def refreshTasktrackers(cls):
        exit_code, output = Hadoop.runas(Config.get('hadoop', 'HDFS_USER'), "mradmin -refreshNodes")
        return output
    
    @classmethod
    def getAttemptIdsForJobId(cls, jobId, myTask='map'):
        listAttemptCmd = " job -list-attempt-ids " + jobId + " " + myTask + " running "
        out = Hadoop.run(listAttemptCmd)
        buf = StringIO.StringIO(out[1])
        return buf.readline().rstrip("\r\n")
