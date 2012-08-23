import time, os
from beaver.component.hadoop import Hadoop, HDFS, MAPRED
from beaver.config import Config
from beaver import util

class CommonHadoopEnv:
    
    @classmethod
    def getCluster(cls):
        return Config.get('hadoop', 'CLUSTER') 
    
    @classmethod
    def getVersion(cls):
        return Config.get('hadoop', 'VERSION') 

    @classmethod
    def getIsSecure(cls):
        _security_prop_value = Hadoop.getConfigValue("hadoop.security.authentication", "kerberos") 
        isSecure = True
        if not _security_prop_value  == "kerberos":
            isSecure =False
        
        return isSecure
    
    @classmethod
    def getArtifactsDir(cls):
        return Config.getEnv('ARTIFACTS_DIR')
    
    @classmethod
    def getHadoopExamplesJar(cls):
        return Config.get('hadoop', 'HADOOP_EXAMPLES_JAR')
    
    @classmethod
    def getHadoopQAUser(cls):
        return Config.get('hadoop', 'HADOOPQA_USER')
    
    @classmethod
    def getKerberosTicketsDir(cls):
        return os.path.join(CommonHadoopEnv.getArtifactsDir(), CommonHadoopEnv.getCluster()+".kerberosTickets."+str(int(time.time())))

    @classmethod
    def getKerberosTicketsSuffix(cls):
        return ".kerberos.ticket"
    
    @classmethod
    def getNNSafemodeTimeout(cls):
        return 300

    @classmethod
    def getHadoopConfDir(cls):
        return Config.get('hadoop', 'HADOOP_CONF')
    
    @classmethod
    def getHDFSUser(cls):
        return Config.get('hadoop', 'HDFS_USER')
       
    @classmethod
    def getMapredUser(cls):
        return Config.get('hadoop', 'MAPRED_USER')