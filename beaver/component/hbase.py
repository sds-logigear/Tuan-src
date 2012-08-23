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
from beaver.machine import Machine
from beaver.config import Config
import os, random

CWD = os.path.dirname(os.path.realpath(__file__))

class HBase:
    @classmethod
    def run(cls, cmd, logoutput=True):
        return Machine.run(Config.get('hbase', 'HBASE_CMD') + " " + cmd, logoutput=logoutput)

    @classmethod
    def runRubyScript(cls, scriptfile, logoutput=False):
        return cls.run("org.jruby.Main " + scriptfile, logoutput=logoutput)

    @classmethod
    def runShellCmds(cls, cmds):
        tfpath = getTempFilepath()
        tf = open(tfpath, 'w')
        for cmd in cmds:
            tf.write(cmd + "\n")
        tf.write("exit\n")
        tf.close()
        output = Machine.run("hbase shell %s" % tfpath)
        os.remove(tfpath)
        return output

    @classmethod
    def dropTable(cls, tablename):
        return cls.runShellCmds(["disable '%s'" % tablename, "drop '%s'" % tablename])

    @classmethod
    def createTable(cls, tablename, columnFamily=None):
        createcmd = "create '%s'" % tablename
        if columnFamily:
            createcmd += ", '%s'" % columnFamily
        return cls.runShellCmds([createcmd])

    @classmethod
    def dropAndCreateTable(cls, tablename, columnFamily=None):
        createcmd = "create '%s'" % tablename
        if columnFamily:
            createcmd += ", '%s'" % columnFamily
        return cls.runShellCmds(["disable '%s'" % tablename, "drop '%s'" % tablename, createcmd])

    @classmethod
    def getTableColumnValues(cls, tablename, columnFamily, column):
        tfpath = getTempFilepath()
        exit_code, output = cls.runRubyScript(" ".join([os.path.join(CWD, 'read_data.rb'), tablename, columnFamily, column, tfpath]))
        if exit_code != 0:
            return []
        output = open(tfpath).readlines()
        os.remove(tfpath)
        return output

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("version")
        if exit_code == 0:
            import re
            pattern = re.compile("VersionInfo: HBase (\S+)")
            m = pattern.search(output)
            if m:
                return m.group(1)
        return ""

def getTempFilepath():
    return os.path.join(Config.getEnv('ARTIFACTS_DIR'), 'tmp-%d' % int(999999*random.random()))
