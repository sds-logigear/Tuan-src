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
import platform
import subprocess
import logging
import os, sys
import shutil
import re
import getpass
import socket
from beaver.config import Config

REC = re.compile(r"(\r\n|\n)$")

logger = logging.getLogger(__name__)

class BaseMachine:
    """Base class for OS"""
    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True):
        cmd = cls._decoratedcmd(cmd)
        logger.info("RUNNING: " + cmd)
        stdout = ""
        osenv = None
        if env:
            osenv = os.environ.copy()
            for key, value in env.items():
                osenv[key] = value
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, cwd=cwd, env=osenv)
        while proc.poll() is None:
            stdoutline = proc.stdout.readline()
            if stdoutline:
                stdout += stdoutline
                if logoutput:
                    logger.info(stdoutline.strip())
        remaining = proc.communicate()
        remaining = remaining[0].strip()
        if remaining != "":
            stdout += remaining
            if logoutput:
                for line in remaining.split("\n"):
                    logger.info(line.strip())
        return proc.returncode, REC.sub("", stdout, 1)

    @classmethod
    def _decoratedcmd(cls, cmd):
        return cmd

    @classmethod
    def sudocmd(cls, cmd, user):
        pass

    @classmethod
    def sshcmd(cls, cmd, host):
        pass

    @classmethod
    def _buildcmd(cls, cmd, user, host):
        if not isLoggedOnUser(user):
            cmd = cls.sudocmd(cmd, user)
        if not isSameHost(host):
            cmd = cls.sshcmd(cmd, host)
        return cmd

    @classmethod
    def runas(cls, user, cmd, host="", cwd=None, env=None, logoutput=True):
        return cls.run(cls._buildcmd(cmd, user, host), cwd, env, logoutput)

    @classmethod
    def runinbackground(cls, cmd,cwd=None, env=None):
        logger.info("RUNNING IN BACKGROUND: " + cmd)
        osenv = None
        if env:
            osenv = os.environ.copy()
            for key, value in env.items():
                osenv[key] = value

        null = open(os.devnull, 'w')
        process = subprocess.Popen(cmd, stdout=null, stderr=null, shell=True, cwd=cwd, env=osenv)
        return process

    @classmethod
    def runinbackgroundAs(cls, user, cmd, host="",cwd=None, env=None):
        return cls.runinbackground(cls._buildcmd(cmd, user, host),cwd, env)

    @classmethod
    def _performcopy(cls, user, host, srcpath, destpath, localdest):
        if isLoggedOnUser(user) and isSameHost(host):
            cls.copy(srcpath, destpath)
        else:
            cls.run(cls._copycmd(user, host, srcpath, destpath, localdest))
 
    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest):
        pass
            
    @classmethod
    def copyToLocal(cls, user, host, srcpath, destpath):
        cls._performcopy(user, host, srcpath, destpath, locadest=True)

    @classmethod
    def copyFromLocal(cls, user, host, srcpath, destpath):
        cls._performcopy(user, host, srcpath, destpath, locadest=False)

    @classmethod
    def copy(cls, srcpath, destpath):
        shutil.copytree(srcpath, destpath)

    @classmethod
    def getProcessList(cls):
        pass

    @classmethod
    def stopService(cls, sname):
        pass

    @classmethod
    def startService(cls, sname):
        pass

    @classmethod
    def findProcess(cls, cmd):
        plist = cls.getProcessList("%a")
        for pitem in plist:
            if pitem.find(cmd) != -1:
                return pitem
        return ""

class LinuxMachine(BaseMachine):
    """Linux machine"""
    @classmethod
    def sudocmd(cls, cmd, user):
        return "sudo su - -c \"%s\" %s" % (cmd, user)

    @classmethod
    def sshcmd(cls, cmd, host):
        return "ssh %s \"%s\"" % (host, cmd)
 
    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest):
        if localdest:
            return "scp -r %s@%s:%s %s" % (user, host, srcpath, destpath)
        else:
            return "scp -r %s %s@%s:%s" % (srcpath, user, host, destpath)
 
    # Get list of running processes
    # Format - Process list format, default user pid ppid cmd
    # Returns a list in the specified format
    @classmethod
    def getProcessList(cls, format="%U %p %P %a"):
        exit_code, stdout = cls.run("ps -eo \"%s\" --no-headers" % format, logoutput = False)
        return stdout.split("\n")

    @classmethod
    def stopService(cls, sname):
        return cls.runas("root", "service %s stop" % sname)

    @classmethod
    def startService(cls, sname):
        return cls.runas("root", "service %s start" % sname)

    @classmethod
    def resetNode(cls, nodename, hostlist,action):
        HADOOP_HOME = Config.get('hadoop', 'HADOOP_HOME')
        HADOOP_BIN = os.path.join(HADOOP_HOME,"bin")

        MAPRED_USER = Config.get('hadoop', 'MAPRED_USER')
        HDFS_USER = Config.get('hadoop', 'HDFS_USER')

        user = { 'namenode': HDFS_USER,
                     'datanode': MAPRED_USER,
                     'jobtracker': HDFS_USER,
                     'tasktracker': MAPRED_USER,
                     }[nodename]
        cmd = os.path.join(HADOOP_BIN,"hadoop-daemon.sh" )
        cmd = cmd + " " +  action + " " + nodename
        
        pattern = "(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        for host in hostlist:
            if not re.search(pattern,host):
                host = host.split(':')[0]
                host = socket.gethostbyname(host.replace('\n','').strip()) 
            
            host = re.findall(pattern, host)[0]
            logger.info( action + " " + nodename + " on host " + host)
            Machine.runinbackgroundAs(user, cmd, host,HADOOP_BIN)

class WindowsMachine(BaseMachine):
    """Windows machine"""
    @classmethod
    def _decoratedcmd(cls, cmd):
        return "call " + cmd

    @classmethod
    def sudocmd(cls, cmd, user):
        return "runas /user:%s %s" % (user, cmd)

    @classmethod
    def sshcmd(cls, cmd, host):
        return cmd

    @classmethod
    def _copycmd(cls, user, host, srcpath, destpath, localdest):
        pass

    @classmethod
    def resetNode(cls, nodename, hostlist,action):
        HADOOP_HOME = Config.get('hadoop', 'HADOOP_HOME')
        HADOOP_BIN = os.path.join(HADOOP_HOME,"bin")
        nodetype = { 'namenode': 'master',
                     'datanode': 'slave',
                     'jobtracker': 'master',
                     'tasktracker': 'slave',
                     }[nodename]

        MAPRED_USER = Config.get('hadoop', 'MAPRED_USER')
        HDFS_USER = Config.get('hadoop', 'HDFS_USER')

        user = { 'namenode': HDFS_USER,
                     'datanode': MAPRED_USER,
                     'jobtracker': HDFS_USER,
                     'tasktracker': MAPRED_USER,
                     }[nodename]
        cmd = os.path.join(HADOOP_BIN,action + "-" + nodetype + ".cmd")
 
        pattern = "(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        for host in hostlist:
            if not re.search(pattern,host):
                host = host.split(':')[0]
                host = socket.gethostbyname(host.replace('\n','').strip()) 

            host = re.findall(pattern, host)[0]
            logger.info( action + " " + nodename + " on host " + host)
            Machine.runinbackgroundAs(user, cmd, host,HADOOP_BIN)
            
            
# Check whether the hosts are same
# host1 - remote host, can take in hostname, fqdn, ipaddress
# host2 - another host, not specifying it will assume itself
def isSameHost(host1, host2=""):
    return not host1 or host1 == "" or socket.getfqdn(host1) == socket.getfqdn(host2)

# Check whether the specified user is logged on user
# user - user to check
def isLoggedOnUser(user):
    return not user or user == "" or getpass.getuser() == user

if platform.system() == 'Windows':
    Machine = WindowsMachine()
else:
    Machine = LinuxMachine()
