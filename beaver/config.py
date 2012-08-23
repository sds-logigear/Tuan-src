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
import os, re
from ConfigParser import ConfigParser
from beaver import util

class Config:
    config = ConfigParser()
    ENV_SECTION = 'env'

    def __call__(self):
        return self

    def parseConfig(self, cfile):
        self.config.read(cfile)
        resolveFuncs(self.config)

    def get(self, section, option):
        return self.config.get(section, option)

    def setEnv(self, env, val):
        if not self.config.has_section(self.ENV_SECTION):
             self.config.add_section(self.ENV_SECTION)
        self.config.set(self.ENV_SECTION, env, val)

    def getEnv(self, env):
        return self.config.get(self.ENV_SECTION, env)

Config = Config()

def resolveFuncs(config):
    FUNCREG = re.compile(r"(\${.*})")
    for section in config.sections():
        for item in config.items(section):
            option, value = item
            m = FUNCREG.match(value)
            if m:
                for group in m.groups():
                    funccall = group[2:-1]
                    out = eval(funccall)
                    value = value.replace(group, out)
            config.set(section, option, value)

def find(basedir, matchstr):
    matches = util.findMatchingFiles(basedir, matchstr)
    if len(matches) > 0:
        return matches[0]
    return ""

def join(*args):
    return os.sep.join(list(args))
