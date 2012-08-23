from beaver import util
from ConfigParser import ConfigParser
from beaver.config import Config
import sys, socket, platform

SECTION = "HW-QE-PUBLISH-REPORT"
COMPONENT_VERSION_MAP = {'Hadoop': 'beaver.component.hadoop.Hadoop', 'HBase': 'beaver.component.hbase.HBase',
                         'FlumeNG': 'beaver.component.flume.FlumeNG'}

def generateTestReportConf(infile, outfile, results):
    config = ConfigParser()
    config.optionxform=str
    config.read(infile)
    if config.has_section(SECTION):
        for option, value in config.items(SECTION):
            if value != "": continue
            elif option == "BUILD_ID" and config.has_option(SECTION, "REPO_URL"):
                config.set(SECTION, option, getBuildId(config.get(SECTION, "REPO_URL")))
                config.remove_option(SECTION, "REPO_URL")
            elif option == "HOSTNAME":
                config.set(SECTION, option, socket.getfqdn())
            elif option == "COMPONENT_VERSION":
                if not config.has_option(SECTION, "COMPONENT") or config.get(SECTION, "COMPONENT") == "":
                    config.set(SECTION, "COMPONENT", "Hadoop")
                config.set(SECTION, option, getComponentVersion(config.get(SECTION, "COMPONENT")))
            elif option == "OS":
                config.set(SECTION, option, platform.platform())
            elif option == "SECURE" and Config.hasOption('hadoop', 'IS_SECURE'):
                config.set(SECTION, option, Config.get('hadoop', 'IS_SECURE').lower())
            elif option == "BLOB":
                pass
            elif option == "RAN":
                config.set(SECTION, option, results[0] + len(results[1]))
            elif option == "PASS":
                config.set(SECTION, option, results[0])
            elif option == "FAIL":
                config.set(SECTION, option, len(results[1]))
            elif option == "SKIPPED":
                config.set(SECTION, option, results[2])
            elif option == "ABORTED":
                config.set(SECTION, option, results[3])
            elif option == "FAILED_TESTS":
                config.set(SECTION, option, ",".join(results[1]))
            elif option == "SINGLE_NODE":
                from beaver.component.hadoop import HDFS
                if HDFS.getDatanodeCount() > 1:
                    config.set(SECTION, option, "false")
                else:
                    config.set(SECTION, option, "true")
        config.write(open(outfile, 'w'))

def getBuildId(url):
    output = util.getURLContents(url + "/build.id")
    return util.getPropertyValue(output, "TIMESTAMP", delimiter=":")

def getComponentVersion(component):
    if not COMPONENT_VERSION_MAP.has_key(component):
        return ""
    try:
        module, clsname = COMPONENT_VERSION_MAP[component].rsplit(".", 1)
        __import__(module)
        imod = sys.modules[module]
        icls = getattr(imod, clsname)
        return getattr(icls, "getVersion")()
    except ImportError, AttributeError:
        return ""

