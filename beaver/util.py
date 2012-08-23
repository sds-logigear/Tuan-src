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
import os
import re
import fnmatch
import string
import shutil
import logging
import datetime
from xml.dom import minidom

logger = logging.getLogger(__name__)

def findMatchingFiles(basedir, matchstr):
    matchFiles = []
    for root, dirnames, filenames in os.walk(basedir):
        for filename in fnmatch.filter(filenames, matchstr):
            matchFiles.append(os.path.join(root, filename))
    return matchFiles

def compareOutputToFile(act_out, exp_file, regex=True, vars={}):
    if not os.path.isfile(exp_file):
        return False
    actLines = act_out
    if not isinstance(act_out, list):
        actLines = actLines.split("\n")
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    return compareLines(actLines, expLines, regex, vars)

def compareFiles(act_file, exp_file, regex=True, vars={}):
    #check if the files are valid
    if not os.path.isfile(act_file) or not os.path.isfile(exp_file):
        return False

    #get the lines in an array
    actLines = [line.rstrip('\r\n') for line in open(act_file).readlines()]
    expLines = [line.rstrip('\r\n') for line in open(exp_file).readlines()]
    return compareLines(actLines, expLines, regex, vars)

def compareLines(act_lines, exp_lines, regex=True, vars={}):
    #count the number of lines and they should match
    actCount = len(act_lines)
    expCount = len(exp_lines)
    if actCount != expCount:
        return False

    #compare line by line
    if regex:
        for count in xrange(expCount):
            matchObj = re.match(replaceVars(exp_lines[count], vars),act_lines[count])
            if not matchObj:
                return False
    else:
        for count in xrange(expCount):
            if replaceVars(exp_lines[count], vars) != act_lines[count]:
                return False
    return True

def replaceVars(text, vars):
    intext = text
    for key, value in vars.items():
        intext = intext.replace("${%s}" % key, value)
    return intext

# Write text into a file
# wtext - Text to write
def writeToFile(wtext, outfile):
    outf = open(outfile, 'w')
    try:
        outf.write(wtext)
    finally:
        outf.close()

def extractAndPlot(textToSearch, regex, plotfile):
    m = re.match(regex, textToSearch, re.DOTALL)
    if not m: return
    value = m.group(1)
    writeToPlotfile(value, plotfile)

def writeToPlotfile(textToWrite, plotfile):
    writeToFile("YVALUE=%s" % textToWrite, plotfile)

# Get the property value from configuration file
# xmlfile - Path of the config file
# name - Property name
# default - Property value in case not found
def getPropertyValueFromConfigXMLFile(xmlfile, name, defaultValue=None):
    xmldoc = minidom.parse(xmlfile)
    propNodes = [node.parentNode for node in xmldoc.getElementsByTagName("name") if node.childNodes[0].nodeValue == name]
    if len(propNodes) > 0:
        for node in propNodes[0].childNodes:
            if node.nodeName == "value":
                return node.childNodes[0].nodeValue
    return defaultValue

# Parse the JUnit testresults and return a flat testcase result map
# junitxmlfile - junit testresults
# output will be of the following form:
# {tcid1: {result: 'pass|fail', failure: ''},...}
def parseJUnitXMLResult(junitxmlfile):
    xmldoc = minidom.parse(junitxmlfile)
    testresult = {}
    tsnodes = xmldoc.getElementsByTagName("testsuite")
    for node in tsnodes:
        tsname = node.getAttribute("name")
        for tschildnode in node.childNodes:
            if tschildnode.nodeType == tschildnode.ELEMENT_NODE and tschildnode.nodeName == "testcase":
                tcid = tsname + "-" + tschildnode.getAttribute("name")
                tcresult = {'result':'pass','failure':''}
                if tschildnode.hasChildNodes():
                    for tccnode in tschildnode.childNodes:
                        if tccnode.nodeType == tccnode.ELEMENT_NODE and tccnode.nodeName == "failure":
                            tcresult['failure'] = tccnode.getAttribute("message")
                            tcresult['result'] = 'fail'
                testresult[str(tcid)] = tcresult
    return testresult

# Update the XML configuration properties and write to another file
# infile - Input config XML file
# outfile - Output config XML file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToConfigXMLFile(infile, outfile, propertyMap):
    xmldoc = minidom.parse(infile)
    cfgnode = xmldoc.getElementsByTagName("configuration")
    if len(cfgnode) == 0:
        raise Exception("Invalid Config XML file: " + infile)
    cfgnode = cfgnode[0]
    propertyMapKeys = propertyMap.keys()
    modified = []
    for node in xmldoc.getElementsByTagName("name"):
        name = node.childNodes[0].nodeValue.strip()
        if name in propertyMapKeys:
            modified.append(name)
            for vnode in node.parentNode.childNodes:
                if vnode.nodeName == "value":
                    vnode.childNodes[0].nodeValue = propertyMap[name]
    remaining = list(set(propertyMapKeys) - set(modified))
    for property in remaining:
        pn = xmldoc.createElement("property")
        nn = xmldoc.createElement("name")
        ntn = xmldoc.createTextNode(property)
        nn.appendChild(ntn)
        pn.appendChild(nn)
        vn = xmldoc.createElement("value")
        vtn = xmldoc.createTextNode(propertyMap[property])
        vn.appendChild(vtn)
        pn.appendChild(vn)
        cfgnode.appendChild(pn)
    writeToFile(xmldoc.toxml(), outfile)

# Get the value for a property in Java style properties file
# does not work for multiline properties
# propfile - property filepath
# key - property name
def getPropertyValueFromFile(propfile, key):
    proptext = open(propfile).read()
    return getPropertyValue(proptext, key)

# Parse a key <delimiter> value combinations in multiline
# does not work for multiline properties
# contents - string to search in
# key - property name to search value for
# delimiter - string separating the key and value
def getPropertyValue(contents, key, delimiter="\\="):
    m = re.search("[^#]*%s%s(.*)\n?" % (key, delimiter), contents)
    if m: return m.group(1).strip()
    return None

# Update the Java style properties file with the key, values from propertyMap
# infile - Input properties file
# outfile - Output properties file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToFile(infile, outfile, propertyMap):
    modified = []
    propstr = ""
    key = ""
    foundslash = False
    for line in open(infile).readlines():
        sline = line.strip()
        if not foundslash:
            if sline.startswith("#"):
                propstr += line
            elif sline.find("=") != -1:
                key, value = sline.split("=", 1)
                key = key.strip()
                if propertyMap.has_key(key):
                    propstr += "%s=%s\n" % (key, propertyMap[key])
                    modified.append(key)
                else:
                    propstr += line
        else:
            if key != "" and not propertyMap.has_key(key):
                propstr += line
        foundslash = sline.endswith("\\")
    remaining = list(set(propertyMap.keys()) - set(modified))
    for property in remaining:
        propstr += "%s=%s\n" % (property, propertyMap[property])
    writeToFile(propstr, outfile)

# Get the content of the URL and optionally write to file
# url - URL
# outfile - output file
def getURLContents(url, outfile=""):
    import urllib2
    try:
        req = urllib2.Request(url)
        u = urllib2.urlopen(req)
        response = u.read()
        u.close()
    except urllib2.URLError:
        response = ""
    if outfile != "": writeToFile(response, outfile)
    return response

# Serializes the URL content into a Python object
# url - URL
def getJSONContent(url):
    contents = getURLContents(url)
    try:
        import json
        contents = json.loads(contents)
    except ImportError:
        contents = eval(contents.replace('null','None').replace('true','True').replace('false','False'))
    return contents

#Create a empty file of the given size
def createFileOfSize(size,filename):
    f = open(filename,"wb")
    f.seek(size-1)
    f.write("\0")
    f.close()

# Function to find matching pattern in a file. Returns 0 if a match is found. 1 otherwise.
def findMatchingPatternInFile(filename, pattern):
    f = open(filename,'r')
    regex = re.compile(pattern)
    for line in f:
        if regex.search(line):
            f.close()
            return 0
    f.close()
    return 1


# Function to copy a directory. If the detination already exists, it will be deleted
def copyDir(srcpath,destpath):
    if os.path.isdir(destpath):
        shutil.rmtree(destpath)
    shutil.copytree(srcpath,destpath)
    
#Added by Logigear, 13-Aug-2012
#Check a pattern contain in text 
def doesContainText(act_text, pattern, regex=True):
    result = False
    if regex:
        if re.search(pattern, act_text):
            result = True
    else:
        if pattern in act_text:
            result = True

    return result

#Function to display test case information before running it
def displayTestCaseMessage(testCaseDesc, testCaseId): 
    today = datetime.date.today()
    messageLog = "\n\n"
    messageLog += "*****************************************************************\n"
    messageLog += "TEST CASE: " + testCaseId + "\n"
    messageLog += "Start testing: %s" % today
    messageLog += "*****************************************************************\n"
    logger.info(messageLog)
