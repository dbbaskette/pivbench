__author__ = 'root'

import glob
import os
import logging
import argparse
import time
from distutils import spawn
import tarfile

import psycopg2
from psycopg2.extras import LoggingConnection
from psycopg2.extras import LoggingCursor
import queries
import wget
import sh
import getpass
from utils import ssh, PackageManager,Hadoop


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

LOG_FILENAME = 'pivotal-benchmark.log'
rootLogger = logging.getLogger("Root Logger")
logHandler = logging.FileHandler(LOG_FILENAME)
logHandler.setFormatter(formatter)
rootLogger.addHandler(logHandler)
rootLogger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

DBLOG_FILENAME = 'db.log'
dbLogger = logging.getLogger("Database Operation Logging")
dblogHandler = logging.FileHandler(DBLOG_FILENAME)
dblogHandler.setFormatter(formatter)
dbLogger.addHandler(dblogHandler)
dbLogger.setLevel(logging.INFO)

#

workingDir = os.getcwd()


# Tables in the TPC-DS schema.
dimensionTables = (
    'date_dim', 'time_dim', 'item', 'customer', 'customer_demographics', 'household_demographics', 'customer_address',
    'store', 'promotion', 'warehouse', 'ship_mode', 'reason', 'income_band', 'call_center', 'web_page', 'catalog_page',
    'web_site')
factTables = (
    'store_sales', 'store_returns', 'web_sales', 'web_returns', 'catalog_sales', 'catalog_returns', 'inventory')



def buildGen():
    currentDir = os.getcwd()
    if not PackageManager.installed(["gcc"]):
        PackageManager.install("gcc")
    if not (spawn.find_executable("mvn")) and not (os.path.isdir("./apache-maven-3.3.1")):
        print "Maven Not Found....Installing"
        maven = wget.download(
            "http://mirror.sdunix.com/apache/maven/maven-3/3.3.1/binaries/apache-maven-3.3.1-bin.tar.gz",
            "apache-maven-3.3.1-bin.tar.gz")
        mavenTar = tarfile.open("./apache-maven-3.3.1-bin.tar.gz", "r:gz")
        mavenTar.extractall(currentDir)
    path = os.getenv("PATH")


    mavenHome = currentDir + "/apache-maven-3.3.1"
    os.environ["MAVEN_HOME"] = mavenHome
    os.environ["PATH"] = path + ":" + mavenHome + "/bin"

    os.chdir(os.getcwd()+"/tpcds-gen")
    print "Building Generator...."
    sh.make("clean")
    sh.make()
    print "Build Complete"




def getSegmentMapping(host):
    hawqURI=queries.uri("10.103.42.155", port=5432, dbname='tpcds', user='gpadmin', password='gpadmin')
    with queries.Session(hawqURI) as session:
        queryString = "select * from gp_segment_configuration where hostname='"+host+"'"
        result = session.query(queryString)
        segments=[]
        for line in result:
            #print line
            if line['content']>=0:
               # print "Host: "+host+"   Seg: gpseg"+str(line['dbid'])
                segments.append(line['content'])
        if len(segments)>0:
            return segments
    return []


def dsdgenPrepare(host,username,password):
    print "Copying Files to "+host
    ssh.putFile(host,"./TPCDSVersion1.3.1/dbgen2/dsdgen","root","password")
    ssh.putFile(host,"./TPCDSVersion1.3.1/dbgen2/tpcds.idx","root","password")
    ssh.exec_command2(host,"root","password",'chmod +x /tmp/dsdgen')

    ssh.exec_command2(host,"root","password",'chmod +x /tmp/tpcds.idx')

    ssh.exec_command2(host,"root","password",'yum -y install gcc')
    ssh.exec_command2(host,"root","password",'rm -f /tmp/*.dat')



def generateData(scale,base,namenode ):
    print "Data Generation"

    if (Hadoop.ls(base))[0] == -1:
        result = Hadoop.mkdir(base)
        if result[0] < 0:
            print result[1]
            exit()
    else:
        print "Base Directory already exists.  Please change and rerun"
        exit()
    buildGen()
    os.chdir(workingDir+"/tpcds-gen")

    for file in glob.glob("target/*.jar"):
        jarFile = file

    print "Data Generation MapRed Job Starting"
    result = Hadoop.run(jarFile,scale,base)
    print "Data Generation MapRed Job Complete"
    print "Changing Replication Factor of RawData to 2"
    result = Hadoop.setrep(2,base)




def createTables(master,database,username,password):
    dbLogger.info( "---------------------------------")
    dbLogger.info( "Creating HAWQ Internal Tables")
    dbLogger.info( "---------------------------------")
    hawqURI=queries.uri(master, port=5432, dbname=database, user=username, password=password)
    tableList = glob.glob('./hawq-ddl/hawq/*.sql')

    with queries.Session(hawqURI) as session:
        for table in tableList:
            ddlFile = open(table,"r")
            tableName = (table.split("/")[3]).split(".")[0]
            print "Creating Table: "+tableName
            tableDDL = ddlFile.read()
            result = session.query(tableDDL)



def createPXFTables(master,database,username,password,scale,base,namenode):

    dbLogger.info( "---------------------------------")
    dbLogger.info( "Creating HAWQ Internal Tables")
    dbLogger.info( "---------------------------------")
    hawqURI=queries.uri(master, port=5432, dbname=database, user=username, password=password)
    tableList = glob.glob('./hawq-ddl/pxf/*.sql')

    with queries.Session(hawqURI) as session:
        for table in tableList:
            ddlFile = open(table,"r")
            tableName = (table.split("/")[3]).split(".")[0]
            print "Creating PXF External Table: "+tableName
            tableDDL = ddlFile.read()
            tableDDL = tableDDL.replace("$NAMENODE",namenode)
            tableDDL = tableDDL.replace("$SCALE",scale)
            tableDDL = tableDDL.replace("$BASE",base[1:])
            result = session.query(tableDDL)





def clearBuffers(hostsFile):

    with open(hostsFile,"r") as hostsFileReader:
        hosts = hostsFileReader.readlines()

    for host in hosts:
        ssh.exec_command2(host.rstrip(),"root","password","free -m;echo 3 > /proc/sys/vm/drop_caches;sync;free -m")


def executeQueries(ipAddress,username,password,queryNum,hostsFile):
    print "Create External Tables"
    dbLogger.info( "---------------------------------")
    dbLogger.info( "Executing HAWQ Queries")
    dbLogger.info( "---------------------------------")
    hawqURI=queries.uri("10.103.42.155", port=5432, dbname='tpcds', user=username, password=password)
    queryList=[]
    if int(queryNum) > 0:
        dbLogger.info("Running Query %s",queryNum)
        if int(queryNum < 10):
            queryNum = "0"+queryNum
        queryList.append('./hawq-ddl/queries/query_'+str(queryNum)+'.sql')
    else:
        dbLogger.info("Running all Queries")
        queryList = glob.glob('./hawq-ddl/queries/*.sql')

    with queries.Session(hawqURI) as session:
        for query in queryList:
            clearBuffers(hostsFile)
            ddlFile = open(query,"r")
            queryName = (query.split("/")[3]).split(".")[0]
            queryString = ddlFile.read()
            startTime = time.time()
            result = session.query(queryString)
            stopTime = time.time()
            queryTime = stopTime - startTime

            dbLogger.info("Query: %s   Execution Time(s): %0.2f  Rows Returned: %s" % (queryName,queryTime,str(result.count())))



def getGpadminCreds():
    gpPassword = getpass.getpass("Password for gpadmin:")
    return gpPassword


def cliParse():
    VALID_ACTION = ["load","gen","query"]
    parser = argparse.ArgumentParser(description='Pivotal HAWQ TPC-DS Loader')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_load = subparsers.add_parser("load", help="Load data into HAWQ")
    parser_gen = subparsers.add_parser("gen", help="Build Data Generator and Generate RAW Data into HDFS")

    parser_query = subparsers.add_parser("query", help="Query the Database")
    parser_query.add_argument("--num", dest='queryNum', action="store", help="Query Number to Execute (0 for all)",
                               required=True)
    parser_query.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                               required=True)
    parser_query.add_argument("--hosts", dest='hostsFile', action="store", help="List of Segment Hosts",
                               required=True)
    parser_load.add_argument("--base", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                               required=True)
    parser_load.add_argument("--scale", dest='scale', action="store",
                               help="Scale:  30000=30TB", required=True)
    parser_load.add_argument("--parquet", dest='format', action="store", help="Store as Parquet Formatted",
                               required=False)
    parser_load.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                               required=True)
    parser_load.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                               required=True)
    # Add HIVE Support Later
    #parser_load.add_argument("--engine", dest='engine', action="store", help="SQL Engine:  hawq/hive/impala/drill",
    #                          required=True)
    parser_gen.add_argument("--scale", dest='scale', action="store", help="Scale:  30000=30TB",
                               required=True)
    parser_gen.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                               required=False)
    parser_gen.add_argument("--base", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                               required=True)

    args = parser.parse_args()
    main(args)



def loadHawqTables(ipAddress,username,password):
    dbLogger.info( "---------------------------------")
    dbLogger.info( "Load HAWQ Internal Tables")
    dbLogger.info( "---------------------------------")

    conn = psycopg2.connect(database="tpcds",host=ipAddress,user=username,password=password,port="5432")
    tables = glob.glob('./hawq-ddl/load/*.sql')
    cur = conn.cursor()
    for table in tables:
        ddlFile = open(table,"r")
        ddlString = ddlFile.read()
        dbLogger.info( "Loading HAWQ Table: "+table)
        cur.execute(ddlString)
        conn.commit()

    cur.close()
    conn.close()


def analyzeHawqTables(ipAddress,username,password):
    dbLogger.info( ("Loading Hawq Internal Tables"))
    logfile = open('db.log', 'a')

    conn = psycopg2.connect(connection_factory=LoggingConnection,database="tpcds",host=ipAddress,user=username,password=password,port="5432")


    conn.initialize(logfile)
    cur = conn.cursor(cursor_factory=LoggingCursor)

    try:
        cur.execute( open("hawq-analyze.sql", "r").read())
        conn.commit()
    except psycopg2.Error as e:
        dbLogger.info( e.pgerror)
        print " "

    cur.close()
    conn.close()


def getDatabase(master,username,password):
    hawqURI=queries.uri(master, port=5432, dbname='gpadmin', user=username, password=password)
    try:
        dbName = raw_input("Please Enter a Name for the HAWQ TPC-DS Database:")
        with queries.Session(hawqURI) as session:
            result = session.query("create database "+dbName)
    except psycopg2.ProgrammingError as e:
        print "Database already exists. It will be used for Data Loading"
    return dbName




def main(args):

   
    username = "gpadmin"



    if (args.subparser_name == "load"):
        print '\n\n\n'
        password = getGpadminCreds()
        database = getDatabase(args.hawqMaster,username,password)
        dbLogger.info( "HAWQ Testing")
        #createTables(args.hawqMaster,database,username,password)
        createPXFTables(args.hawqMaster,database,username,password,args.scale,args.base,args.namenode)
        #loadHawqTables(args.hawqMaster,username,password)
    elif (args.subparser_name =="gen"):
        generateData(args.scale,args.base,args.namenode)
    elif (args.subparser_name =="query"):
        logging.info( "Query")
        executeQueries(args.hawqMaster,"gpadmin","gpadmin",args.queryNum,args.hostsFile)





if __name__ == '__main__':
    cliParse()