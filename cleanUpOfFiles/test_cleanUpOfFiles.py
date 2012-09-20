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
from beaver.component.hadoop import Hadoop, HDFS, MAPRED
from beaver.config import Config
from beaver.machine import Machine
from beaver import component
from beaver import util
import os
import logging
import pytest
import sys
import time
import StringIO,a
import platform
from beaver.component.common_hadoop_env import CommonHadoopEnv

#Get user from config file
HADOOPQA_USER = CommonHadoopEnv.getHadoopQAUser()
HDFS_USER = CommonHadoopEnv.getHDFSUser()
MAPRED_USER = CommonHadoopEnv.getMapredUser()

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_PATH, "data")
CREATE_FILE = "CreateFile.py"
CREATE_FILE_PATH_IN_HADOOP = '/user/' + HADOOPQA_USER + '/' + CREATE_FILE
CREATE_FILE_PATH_IN_LOCAL = os.path.join(SCRIPT_PATH, "data", CREATE_FILE)
CREATE_FILE_2 = "CreateFile2.py"
CREATE_FILE_2_PATH_IN_HADOOP = '/user/' + HADOOPQA_USER + '/' + CREATE_FILE_2
CREATE_FILE_2_PATH_IN_LOCAL = os.path.join(SCRIPT_PATH, "data", CREATE_FILE_2)
OUT_PATH_IN_HADOOP = '/user/' + HADOOPQA_USER + '/out1'
HADOOP_STREAMING_JAR = Config.get('hadoop', 'HADOOP_STREAMING_JAR')

logger = logging.getLogger(__name__)
  
def validateJobId(jobId):
    jobId = jobId + ""
    return jobId.startswith('job_')

def getLocalDirInfo(host):            
    return util.getPropertyValueFromConfigXMLFile(os.path.join(Config.get('hadoop', 'HADOOP_CONF'), "mapred-site.xml"), "mapred.local.dir")

def checkJobCreatedTempFileInTT(logFileDir, currentUser, currentJobId, currentAttemptId, logfile, taskTrackerHost):      
    pathFile = os.path.join(logFileDir, 'taskTracker', currentUser, 'jobcache', currentJobId, currentAttemptId, 'work', logfile)
    logger.info("path file: " + pathFile)
    result = False
    if platform.system() == 'Windows':
        result = os.path.isfile(pathFile)
    else:
        if CommonHadoopEnv.getIsSecure():
            result = os.path.isfile(pathFile)
        else:
            cmd = "ls %s" % pathFile
            sudocmd = Machine.sudocmd(cmd,MAPRED_USER)
            sudocmd += "|wc -l"
            logger.info("sudocmd = " + sudocmd)
            out = Machine.run(sudocmd)            
            result =  (out[0] == 0 and out[1] == "1")
    return result

def getAttemptIdsForJobIdAndStoreInFile(jobId, myTask="map"):
    artifactsDir = CommonHadoopEnv.getArtifactsDir()
    saveFilePath = os.path.join(artifactsDir,"AttemptIdFile")  
    listAttemptCmd = " job -list-attempt-ids "+ jobId +" "+ myTask + " running " 
    out=Hadoop.run(listAttemptCmd)
    buf = StringIO.StringIO(out[1])    
    util.writeToFile(out[1],saveFilePath)
  
def insertFileIntoHdfs(fileName):
    pathFileName = '/user/' + HADOOPQA_USER + '/' + fileName    
    if (not(HDFS.fileExists(pathFileName))):
        sourceFile = DATA_PATH + '/' + fileName
        destFile = '/user/' + HADOOPQA_USER + '/' + fileName
        putCmd = "dfs -put " + sourceFile + ' ' + destFile
        out = Hadoop.run(putCmd)
        return out

def getFullPathOfFile(filePath):
    localFilePath = filePath.replace("\\", "/")
    if localFilePath.startswith("/"):
        localFilePath = "file://" + localFilePath
    else:
        localFilePath = "file:///" + localFilePath
    return localFilePath         
               
def setup():
    out = HDFS.deleteFile(CREATE_FILE_PATH_IN_HADOOP, user=HDFS_USER)
    assert out[0] == 0    
    out = HDFS.deleteDirectory(OUT_PATH_IN_HADOOP, user=HDFS_USER)
    assert out[0] == 0        
    out = HDFS.deleteDirectory(CREATE_FILE_2_PATH_IN_HADOOP, user=HDFS_USER)
    assert out[0] == 0
    
def test_CleanUpOfFilesAfterJobCompletion():
    testCaseDescription = "testCleanUpOfFilesAfterJobCompletion"
    testCaseId = "cleanup01"
    util.displayTestCaseMessage(testCaseDescription,testCaseId)
    out = insertFileIntoHdfs(CREATE_FILE)
    assert out[0] == 0
    
    logger.info("Try to get Job Tracker")
    JOBTRACKER = MAPRED.getJobTracker()
    assert JOBTRACKER != None
    
    localFilePath = getFullPathOfFile(CREATE_FILE_PATH_IN_LOCAL)
    
    hadoopStreamingCmdFormat = 'jar %s -files %s -input %s -output %s -mapper "python %s" -reducer NONE'
    jobJarHadoopStreamingCmd = hadoopStreamingCmdFormat % (HADOOP_STREAMING_JAR, localFilePath, CREATE_FILE_PATH_IN_HADOOP, OUT_PATH_IN_HADOOP, CREATE_FILE)
    logger.info(jobJarHadoopStreamingCmd)    
    out = Hadoop.runInBackground(jobJarHadoopStreamingCmd)    
    time.sleep(20)
    
    logger.info("Try to get job id.....")    
    for i in range(1, 5):
        jobId = MAPRED.getJobID()        
        if (validateJobId(jobId)):            
            break
        time.sleep(10)
    assert jobId.startswith('job_') == True
    logger.info(" Get JobId: " + jobId + " successfully")
    
    logger.info("Try to get Attempt ID....")   
    attemptId = MAPRED.getAttemptIdsForJobId(jobId)                
    assert attemptId.startswith("attempt_") == True        
    
    logger.info("Try to get Task Tracker...")    
    taskTrackersList = Hadoop.getTasktrackers()
    taskTracker = taskTrackersList[0].rstrip("\n")
    logger.info(" Task Tracker running the map task is " + taskTracker)
    time.sleep(20)
    
    logFileDirList = getLocalDirInfo(taskTracker);
    logger.info("Log file list: " + logFileDirList)
    logFileDirList = logFileDirList.split(',')
    isExistedTempFile = False
    for logFileDir in logFileDirList:        
        logger.info("Directory of log file: " + logFileDir)
        isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir, HADOOPQA_USER, jobId, attemptId, CREATE_FILE, taskTracker)
        if isExistedTempFile == True:            
            break
    assert isExistedTempFile == True
    
    logger.info("Check job is completed or not")
    for i in range(1, 10):
        isJobCompleted = MAPRED.checkForJobCompletion(jobId)
        if isJobCompleted == True:
            break
        time.sleep(20)
    assert isJobCompleted == True
    logger.info("Job is completed!")
    
    logger.info("Check for the file to be cleared off after the job is completed")
    isTempFileCleaned = checkJobCreatedTempFileInTT(logFileDir, HADOOPQA_USER, jobId, attemptId, CREATE_FILE, taskTracker)
    assert isTempFileCleaned == False
    logger.info("Ok, the files are cleared!")
    
def test_CleanUpOfFilesAfterKilledJob():
    testCaseDescription="testCleanUpOfFilesAfterKilledJob"
    testCaseId = "cleanup02"
    util.displayTestCaseMessage(testCaseDescription,testCaseId)
    fileCreated="FileCreatedByJob.log"
    out=insertFileIntoHdfs(CREATE_FILE)
    logger.info("Try to get Job Tracker")
    JOBTRACKER = MAPRED.getJobTracker()
    assert JOBTRACKER != None
    
    logger.info( " Submitting a streaming job that will create a file ")
    localFilePath = getFullPathOfFile(CREATE_FILE_PATH_IN_LOCAL)
    
    hadoopStreamingCmdFormat = 'jar %s -files %s -input %s -output %s -mapper "python %s" -reducer NONE'
    jobJarHadoopStreamingCmd = hadoopStreamingCmdFormat % (HADOOP_STREAMING_JAR, localFilePath, CREATE_FILE_PATH_IN_HADOOP, OUT_PATH_IN_HADOOP, CREATE_FILE)
    logger.info(jobJarHadoopStreamingCmd)    
    out = Hadoop.runInBackground(jobJarHadoopStreamingCmd)    
    time.sleep(20)

    logger.info("Try to get job id.....")    
    for i in range(1, 5):
        jobId = MAPRED.getJobID()        
        if (validateJobId(jobId)):            
            break
        time.sleep(10)
    assert jobId.startswith('job_') == True
    logger.info(" Get JobId: " + jobId + " successfully")
    
    logger.info("Try to get Attempt ID....")   
    attemptId = MAPRED.getAttemptIdsForJobId(jobId)                
    assert attemptId.startswith("attempt_") == True        
    
    logger.info("Try to get Task Tracker...")    
    taskTrackersList = Hadoop.getTasktrackers()
    taskTracker = taskTrackersList[0].rstrip("\n")
    logger.info(" Task Tracker running the map task is " + taskTracker)
    time.sleep(20)
    
    logFileDirList = getLocalDirInfo(taskTracker);
    logger.info("Log file list: " + logFileDirList)
    logFileDirList = logFileDirList.split(',')
    isExistedTempFile = False
    for logFileDir in logFileDirList:        
        logger.info("Directory of log file: " + logFileDir)
        isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir, HADOOPQA_USER, jobId, attemptId, fileCreated, taskTracker)
        if isExistedTempFile == True:            
            break
    assert isExistedTempFile == True
    # Now kill the job
    MAPRED.killAJob(jobId)
    logger.info("Check job exists")    
    isJobExists=MAPRED.isJobExists(jobId)
    if isJobExists==True:
        logger.info( " The job could not be failed successfully and unable to proceed with the tests ")
    assert isJobExists==True
    time.sleep(20)
    
    isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir,HADOOPQA_USER,jobId,attemptId,fileCreated,taskTracker)
    if isExistedTempFile:
        logger.info(" The test case  to check the files cleared after killing of jobs failed ")
        logger.info(" The file created by the job still exists even after the job is successfully killed")
    assert isExistedTempFile==False 
                      
def test_CleanUpOfFilesAfterFailedJob():
    testCaseDescription="testCleanUpOfFilesAfterFailedJob"
    testCaseId = "cleanup03"
    util.displayTestCaseMessage(testCaseDescription,testCaseId)
    fileCreated="FileCreatedByJob.log"
    out=insertFileIntoHdfs(CREATE_FILE)
    assert out[0] == 0
        
    logger.info("Try to get Job Tracker")
    JOBTRACKER = MAPRED.getJobTracker()
    assert JOBTRACKER != None
    
    logger.info( "Submitting a streaming job that will create a file ")
    localFilePath = getFullPathOfFile(CREATE_FILE_PATH_IN_LOCAL)
    
    hadoopStreamingCmdFormat = 'jar %s -files %s -input %s -output %s -mapper "python %s" -reducer NONE'
    jobJarHadoopStreamingCmd = hadoopStreamingCmdFormat % (HADOOP_STREAMING_JAR, localFilePath, CREATE_FILE_PATH_IN_HADOOP, OUT_PATH_IN_HADOOP, CREATE_FILE)
    logger.info(jobJarHadoopStreamingCmd)    
    out = Hadoop.runInBackground(jobJarHadoopStreamingCmd)    
    time.sleep(20)
    
    logger.info("Try to get job id.....")    
    for i in range(1, 5):
        jobId = MAPRED.getJobID()        
        if (validateJobId(jobId)):            
            break
        time.sleep(10)
    assert jobId.startswith('job_') == True
    logger.info(" Get JobId: " + jobId + " successfully")
    
    logger.info("Try to get Attempt ID....")   
    attemptId = MAPRED.getAttemptIdsForJobId(jobId)                
    assert attemptId.startswith("attempt_") == True        
    
    logger.info("Try to get Task Tracker...")    
    taskTrackersList = Hadoop.getTasktrackers()
    taskTracker = taskTrackersList[0].rstrip("\n")
    logger.info(" Task Tracker running the map task is " + taskTracker)
    time.sleep(20)
    
    logFileDirList = getLocalDirInfo(taskTracker);
    logger.info("Log file list: " + logFileDirList)
    logFileDirList = logFileDirList.split(',')
    isExistedTempFile = False
    for logFileDir in logFileDirList:        
        logger.info("Directory of log file: " + logFileDir)
        isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir, HADOOPQA_USER, jobId, attemptId, fileCreated, taskTracker)
        if isExistedTempFile == True:            
            break
    assert isExistedTempFile == True
    # Now fail the job
    getAttemptIdsForJobIdAndStoreInFile(jobId)
    attemptIdCount=MAPRED.checkForNewAttemptIds(jobId)
    assert len(attemptIdCount) != 0        
    while len(attemptIdCount) != 0:
        logger.info(" Since there are  attempts ids  proceeding to kill them ")
        MAPRED.failAttempts(attemptIdCount)
        attemptIdCount=MAPRED.checkForNewAttemptIds(jobId)
        
    logger.info("Check job status")    
    isJobFailed=MAPRED.isJobFailed(jobId)
    if isJobFailed==False:
        logger.info( " The job could not be failed successfully and unable to proceed with the tests ")
    assert isJobFailed==True
    
    isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir,HADOOPQA_USER,jobId,attemptId,fileCreated,taskTracker)
    if isExistedTempFile:
        logger.info(" The test case  to check the files cleared after killing of jobs failed ")
        logger.info(" The file created by the job still exists even after the job is successfully killed ")
    assert isExistedTempFile==False

def test_CleanUpOfFilesAfterJobCompletionForFilesWithSymLink():
    testCaseDescription="test_CleanUpOfFilesAfterJobCompletionForFilesWithSymLink"
    testCaseId = "cleanup04"
    util.displayTestCaseMessage(testCaseDescription,testCaseId)
    fileCreated = "mysymlink.txt"    
    
    out = insertFileIntoHdfs(CREATE_FILE_2)    
    assert out[0] == 0
    time.sleep(15)
    
    logger.info("Try to get Job Tracker")
    JOBTRACKER = MAPRED.getJobTracker()
    assert JOBTRACKER != None
    
    localFilePath = getFullPathOfFile(CREATE_FILE_2_PATH_IN_LOCAL)
    
    hadoopStreamingCmdFormat = 'jar %s -files %s -input %s -output %s -mapper "python %s" -reducer NONE'
    jobJarHadoopStreamingCmd = hadoopStreamingCmdFormat % (HADOOP_STREAMING_JAR, localFilePath, CREATE_FILE_2_PATH_IN_HADOOP, OUT_PATH_IN_HADOOP, CREATE_FILE_2)
    logger.info(jobJarHadoopStreamingCmd)    
    out = Hadoop.runInBackground(jobJarHadoopStreamingCmd)    
    time.sleep(15)
    
    logger.info("Try to get job id.....")    
    for i in range(1, 5):
        jobId = MAPRED.getJobID()        
        if (validateJobId(jobId)):            
            break
        time.sleep(10)
    assert jobId.startswith('job_') == True
    logger.info(" Get JobId: " + jobId + " successfully")
    
    logger.info("Try to get Attempt ID....")   
    attemptId = MAPRED.getAttemptIdsForJobId(jobId)                
    assert attemptId.startswith("attempt_") == True
    
    logger.info("Try to get Task Tracker...")    
    taskTrackersList = Hadoop.getTasktrackers()
    taskTracker = taskTrackersList[0].rstrip("\n")
    logger.info(" Task Tracker running the map task is " + taskTracker)
    time.sleep(40)
    
    logFileDirList = getLocalDirInfo(taskTracker);
    logger.info("Log file list: " + logFileDirList)
    logFileDirList = logFileDirList.split(',')
    isExistedTempFile = False
    for logFileDir in logFileDirList:        
        logger.info("Directory of log file: " + logFileDir)
        isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir, HADOOPQA_USER, jobId, attemptId, fileCreated, taskTracker)
        if isExistedTempFile == True:            
            break
    assert isExistedTempFile == True
    
    logger.info("Check job is completed or not")
    for i in range(1, 10):
        isJobCompleted = MAPRED.checkForJobCompletion(jobId)
        if isJobCompleted == True:
            break
        time.sleep(20)
    assert isJobCompleted == True
    logger.info("Job is completed!")
    
    #  Now check for the file to be cleared off  after the job is complete
    isExistedTempFile = checkJobCreatedTempFileInTT(logFileDir,HADOOPQA_USER,jobId,attemptId,fileCreated,taskTracker)
    if isExistedTempFile == True:
        logger.info(" The test case  to check the files cleared after killing of jobs failed ")
        logger.info(" The file created by the job still exists even after the job is successfully killed ")
    assert isExistedTempFile==False
    
    
