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

class FlumeNG:
    @classmethod
    def runas(cls, user, cmd, cwd=None, env=None, logoutput=True):
        flume_cmd = Config.get('flume-ng', 'FLUME_CMD')
        flume_cmd += " " + cmd
        return Machine.runas(user, flume_cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def run(cls, cmd, cwd=None, env=None, logoutput=True):
        return cls.runas(None, cmd, cwd=cwd, env=env, logoutput=logoutput)

    @classmethod
    def runInBackgroundAs(cls, user, cmd, cwd=None, env=None):
        flume_cmd = Config.get('flume-ng', 'FLUME_CMD')
        flume_cmd += " " + cmd
        return Machine.runinbackgroundAs(user, flume_cmd, cwd=cwd, env=env)

    @classmethod
    def runAgent(cls, name, conffile, user=None, cwd=None, env=None):
        flume_conf = Config.get('flume-ng', 'FLUME_CONF')
        cmd = "agent -n %s -c %s -f %s" % (name, flume_conf, conffile)
        return cls.runInBackgroundAs(user, cmd, cwd=cwd, env=env)

    @classmethod
    def getVersion(cls):
        exit_code, output = cls.run("version")
        if exit_code == 0:
            import re
            pattern = re.compile("^Flume (\S+)")
            m = pattern.search(output)
            if m:
                return m.group(1)
        return ""
