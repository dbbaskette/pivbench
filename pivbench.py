__author__ = 'root'

import glob
import os
import logging
import argparse
import time
from distutils import spawn
import tarfile
import getpass
import datetime
from multiprocessing import Process

import psycopg2
import queries
import wget
import sh
import paramiko

from utils import ssh, PackageManager, Hadoop, Email

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
if not os.path.exists("./logs"):
    os.makedirs("./logs")
LOG_FILENAME = 'pivotal-benchmark.log'
rootLogger = logging.getLogger("Root Logger")
logHandler = logging.FileHandler(LOG_FILENAME)
logHandler.setFormatter(formatter)
rootLogger.addHandler(logHandler)
rootLogger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

PBLOG_FILENAME = './logs/pivbench.log'
pbLogger = logging.getLogger("Database Operation Logging")
pblogHandler = logging.FileHandler(PBLOG_FILENAME)
pblogHandler.setFormatter(formatter)
pbLogger.addHandler(pblogHandler)
pbLogger.setLevel(logging.INFO)

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

    os.chdir(os.getcwd() + "/tpcds-gen")
    print "Building Generator...."
    sh.make("clean")
    sh.make()
    print "Build Complete"


#
# def getSegmentMapping(host):
#     hawqURI=queries.uri("10.103.42.155", port=5432, dbname='tpcds', user='gpadmin', password='gpadmin')
#     with queries.Session(hawqURI) as session:
#         queryString = "select * from gp_segment_configuration where hostname='"+host+"'"
#         result = session.query(queryString)
#         segments=[]
#         for line in result:
#             #print line
#             if line['content']>=0:
#                # print "Host: "+host+"   Seg: gpseg"+str(line['dbid'])
#                 segments.append(line['content'])
#         if len(segments)>0:
#             return segments
#     return []
#
#
# def dsdgenPrepare(host,username,password):
#     print "Copying Files to "+host
#     ssh.putFile(host,"./TPCDSVersion1.3.1/dbgen2/dsdgen","root","password")
#     ssh.putFile(host,"./TPCDSVersion1.3.1/dbgen2/tpcds.idx","root","password")
#     ssh.exec_command2(host,"root","password",'chmod +x /tmp/dsdgen')
#
#     ssh.exec_command2(host,"root","password",'chmod +x /tmp/tpcds.idx')
#
#     ssh.exec_command2(host,"root","password",'yum -y install gcc')
#     ssh.exec_command2(host,"root","password",'rm -f /tmp/*.dat')

def scaleConverter(scale):
    if ("TB" in str.upper(scale)):
        scaleNum = int(scale[:-2]) * 1024
    else:
        scaleNum = int(scale[:-2])
    return scaleNum


def generateData(scale, base, namenode, tableName=""):
    loggerInfo = buildReportLogger("gen")
    reportName = loggerInfo[0]
    report = loggerInfo[1]
    trueScale = scaleConverter(scale)
    uniInfoLog("Creating Dataset for " + str(trueScale), report)
    print str(trueScale)
    if (Hadoop.ls(base))[0] == -1:
        result = Hadoop.mkdir(base)
        if result[0] < 0:
            print result[1]
            exit()
    else:
        print "Base Directory already exists.  Please change and rerun"
        exit()
    buildGen()
    os.chdir(workingDir + "/tpcds-gen")

    for file in glob.glob("target/*.jar"):
        jarFile = file

    uniInfoLog("Data Generation MapRed Job Starting", report)
    # TABLE GEN DOESN"T WORK, SO COMMENTING THIS OUT TO KEEP FROM RUNNING IT
    # if tableName:
    #     result = Hadoop.runTable(jarFile, scale, base, tableName)
    # else:
    #     result = Hadoop.run(jarFile, scale, base)
    result = Hadoop.run(jarFile, str(trueScale), base)
    uniInfoLog("Data Generation MapRed Job Complete", report)
    uniInfoLog("Changing Replication Factor of RawData to 2")
    result = Hadoop.setrep(2, base)


def createTables(master, database, username, password):
    pbLogger.info("---------------------------------")
    pbLogger.info("Creating HAWQ Internal Tables")
    pbLogger.info("---------------------------------")
    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    tableList = sorted(glob.glob('./hawq-ddl/hawq/*.sql'))

    with queries.Session(hawqURI) as session:
        for table in tableList:
            ddlFile = open(table, "r")
            tableName = (table.split("/")[3]).split(".")[0]
            print "Creating Table: " + tableName
            tableDDL = ddlFile.read()
            result = session.query(tableDDL)


def createPXFTables(master, database, username, password, scale, base, namenode):
    pbLogger.info("---------------------------------")
    pbLogger.info("Creating HAWQ PXF External Tables")
    pbLogger.info("---------------------------------")
    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    tableList = sorted(glob.glob('./hawq-ddl/pxf/*.sql'))

    with queries.Session(hawqURI) as session:
        for table in tableList:
            ddlFile = open(table, "r")
            tableName = (table.split("/")[3]).split(".")[0]
            print "Creating PXF External Table: " + tableName
            tableDDL = ddlFile.read()
            tableDDL = tableDDL.replace("$NAMENODE", namenode)
            tableDDL = tableDDL.replace("$SCALE", scale)
            tableDDL = tableDDL.replace("$BASE", base[1:])
            result = session.query(tableDDL)


def clearBuffers(hostsFile, adminUser, adminPassword):
    threadList = []
    with open(hostsFile, "r") as hostsReader:
        hosts = hostsReader.readlines()
    for host in hosts:
        p = Process(target=ssh.exec_command2, args=(
            host.rstrip(), adminUser, adminPassword, "free -m;echo 3 > /proc/sys/vm/drop_caches;sync;free -m"), )
        threadList.append(p)

    for thread in threadList:
        thread.start()

    for thread in threadList:
        thread.join()

    return "Buffers Cleared on All Cluster Nodes"


def buildReportLogger(name):
    reportName = "./logs/" + name + "-" + str(datetime.datetime.now().isoformat('-')) + ".log"
    reportLogger = logging.getLogger("Reporting Logger")
    reportlogHandler = logging.FileHandler(reportName)
    reportlogHandler.setFormatter(formatter)
    reportLogger.addHandler(reportlogHandler)
    reportLogger.setLevel(logging.INFO)
    return (reportName, reportLogger)


def uniInfoLog(msg, logger):
    # for now uses db and user defined
    pbLogger.info(msg)
    logger.info(msg)


def executeQueries(master, database, username, password, queryList, hostsFile, explain,adminUser, adminPassword, scale,
                   emailAddress=""):
    loggerInfo = buildReportLogger("queries")
    reportName = loggerInfo[0]
    report = loggerInfo[1]
    header = []
    explainString=""
    if explain:
        explainString = "explain analyze "
    startString = "Query Execution Phase"
    uniInfoLog(startString, report)
    header = "Executing HAWQ Queries for a "+str(scale)+" Data Set"
    uniInfoLog(header, report)
    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    queryLocations = []

    if int(int(queryList[0])) <> 0:

        # Loop
        for queryNum in queryList:
            uniInfoLog("Running Query " + queryNum, report)

            if int(queryNum) < 10:
                queryNum = "0" + queryNum
            queryLocations.append('./hawq-ddl/queries/"+scale+"/query_' + str(queryNum) + '.sql')
    else:
        uniInfoLog("Running all Queries", report)
        queryLocations = sorted(glob.glob("./hawq-ddl/queries/" + scale + "/*.sql"))

    with queries.Session(hawqURI) as session:
        for query in queryLocations:
            uniInfoLog(clearBuffers(hostsFile, adminUser, adminPassword), report)
            ddlFile = open(query, "r")
            queryName = ((query.split("/")[4]).split(".")[0]).rstrip()
            queryString = ddlFile.read()
            queryString = explainString+queryString
            startTime = time.time()
            result = session.query(queryString)
            if explain:
                queryPlan=""
                uniInfoLog(result.query,report)
                for row in result:
                    queryPlan = queryPlan+str(row['QUERY PLAN']+"\n")
                uniInfoLog(queryPlan,report)
            stopTime = time.time()
            queryTime = stopTime - startTime
            results = "Query Complete: %s   Execution Time(s): %0.2f  Rows Returned: %s" % (
                queryName, queryTime, str(result.count()))
            uniInfoLog(results, report)
            if emailAddress:
                Email.sendEmail(emailAddress, results[:25], results)
        if (emailAddress):
            messageLines = []
            with open(reportName, "r") as reportMsg:
                for line in reportMsg.readlines():
                    messageLines.append(line)
                message = " ".join(messageLines)
                Email.sendEmail(emailAddress, "Query Final Report: " + (reportName.split('/')[2])[:-4], message)


def getGpadminCreds(master):
    gpPassword = getpass.getpass("Password for gpadmin:")
    # verify login
    # hawqURI=queries.uri(master, 5432, dbname="gpadmin", user="gpadmin", password=gpPassword)
    # with queries.Session(hawqURI) as session:

    return gpPassword


def getAdminCreds(hostFile, adminUser):
    password = getpass.getpass("Password for " + adminUser + " (needed for Buffer Clears):")

    with open(hostFile, "r") as hostsReader:
        hosts = hostsReader.readlines()

    for host in hosts:
        print "Testing access to " + host
        try:
            ssh.exec_command2(host.rstrip(), adminUser, password, "touch /.test")
        except paramiko.AuthenticationException as e:
            print "Error:  Username/password not correct"
            exit()
        except Exception as e2:
            print "Error:  Unknown Host"
            exit()

    return password


def cliParse():
    VALID_ACTION = ["load", "gen", "query", "part", "analyze"]
    parser = argparse.ArgumentParser(description='Pivotal HAWQ TPC-DS Loader')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_load = subparsers.add_parser("load", help="Load data into HAWQ")
    parser_gen = subparsers.add_parser("gen", help="Build Data Generator and Generate RAW Data into HDFS")
    parser_part = subparsers.add_parser("part", help="Partition the Fact Tables")
    parser_analyze = subparsers.add_parser("analyze", help="Analyze All Tables")
    parser_clean = subparsers.add_parser("clean", help="Clean Up Data")
    parser_query = subparsers.add_parser("query", help="Query the Database")
    parser_report = subparsers.add_parser("report", help="Configuration Report")

    parser_query.add_argument("--num", dest='queryNum', action="store", help="Query Number to Execute (0 for all)",
                              required=False)
    parser_query.add_argument("--db", dest='database', action="store", help="Database to Query",
                              required=True)
    parser_query.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                              required=True)
    parser_query.add_argument("--hosts", dest='hostsFile', action="store", help="List of Segment Hosts",
                              required=True)
    parser_query.add_argument("--set", dest='querySet', action="store", help="File with a QuerySet (CSV)",
                              required=False)
    parser_query.add_argument("--email", dest='emailAddress', action="store", help="Email Address for Reports",
                              required=False)
    parser_query.add_argument("--admin", dest='adminUser', action="store", help="User with Admin Prics (root)",
                              required=False, default="root")
    parser_query.add_argument("--explain", help="Enable Explain Plan Output",action="store_true",required=False)

    parser_query.add_argument("--scale", dest='scale', action="store", help="User with Admin Prics (root)",
                              required=True)
    parser_load.add_argument("--base", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                             required=True)
    parser_load.add_argument("--scale", dest='scale', action="store",
                             help="Scale: 1GB,5GB,10GB,100GB,1TB,3TB,5TB,10TB,30TB,100TB", required=True)

    # parser_load.add_argument("--orientation", dest='orientation', action="store", help="row/column/parquet",
    #                          required=False)
    parser_load.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                             required=True)
    parser_load.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                             required=True)
    parser_load.add_argument("--db", dest='database', action="store", help="Database to Load",
                             required=True)
    parser_load.add_argument("--email", dest='emailAddress', action="store", help="Email Address for Reports",
                             required=False)
    # Add HIVE Support Later
    # parser_load.add_argument("--engine", dest='engine', action="store", help="SQL Engine:  hawq/hive/impala/drill",
    #                          required=True)
    parser_gen.add_argument("--scale", dest='scale', action="store",
                            help="Scale: 1GB,5GB,10GB,100GB,1TB,3TB,5TB,10TB,30TB,100TB",
                            required=True)
    parser_gen.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                            required=False)
    parser_gen.add_argument("--base", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                            required=True)
    # parser_gen.add_argument("--table", dest='tableName', action="store", help="Gen a particular Table",required=False)


    parser_part.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                             required=True)
    parser_part.add_argument("--num", dest='parts', action="store", help="Number of Partitions",
                             required=True)
    parser_part.add_argument("--db", dest='database', action="store", help="Database with Tables",
                             required=True)
    parser_part.add_argument("--email", dest='emailAddress', action="store", help="Email Address for Reports",
                             required=False)
    parser_part.add_argument("--orientation", dest='orientation', action="store", help="Storage Format",
                             required=False)
    parser_part.add_argument("--rowgroupsize", dest='rowGroupSize', action="store", help="Parquet Row Group Size",
                             required=False)
    parser_part.add_argument("--compression", action="store_true", help="Enable Snappy Compression",
                             required=False)
    parser_part.add_argument("--bypart", action="store_true", help="Load by Partition",
                             required=False)
    parser_analyze.add_argument("--master", dest='hawqMaster', action="store", help="HAWQ Master",
                                required=True)
    parser_analyze.add_argument("--db", dest='database', action="store", help="Database with Tables",
                                required=True)
    parser_analyze.add_argument("--email", dest='emailAddress', action="store", help="Email Address for Reports",
                                required=False)

    parser_clean.add_argument("--scale", dest='scale', action="store",
                              help="Scale: 1GB,5GB,10GB,100GB,1TB,3TB,5TB,10TB,30TB,100TB",
                              required=True)
    parser_clean.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                              required=True)
    parser_clean.add_argument("--base", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                              required=True)
    parser_clean.add_argument("--raw", dest='raw', action="store", help="Clean up Raw Data",
                              required=True)
    parser_clean.add_argument("--", dest='base', action="store", help="Base HDFS Directory for Raw Data",
                              required=True)
    parser_report.add_argument("--namenode", dest='namenode', action="store", help="Namenode Address",
                               required=True)
    parser_report.add_argument("--dir", dest='hdfsDir', action="store", help="HDFS Directory for Reporting",
                               required=True)


    #  FINISH WORK ON CLEAN

    args = parser.parse_args()
    main(args)


def loadHawqTables(master, username, password, database, emailAddress):
    loggerInfo = buildReportLogger("load")
    reportName = loggerInfo[0]
    report = loggerInfo[1]
    uniInfoLog("Load HAWQ Internal Tables", report)

    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    loadList = sorted(glob.glob('./hawq-ddl/load/*.sql'))

    for load in loadList:
        ddlFile = open(load, "r")
        tableName = ((load.split("/")[3]).split(".")[0])[:-5]
        loadDDL = ddlFile.read()
        startTime = datetime.datetime.now()
        pxfName = tableName
        if tableName in factTables:
            tableName = tableName + "_nopart"
        uniInfoLog("Starting Load of " + tableName, report)
        with queries.Session(hawqURI) as session:
            result = session.query(loadDDL)
        stopTime = datetime.datetime.now()
        uniInfoLog("Completed Load of " + tableName, report)
        uniInfoLog("Load Time: " + str(stopTime - startTime), report)
        rowsPXF = rowCount(master, database, username, password, pxfName + "_pxf")
        rows = rowCount(master, database, username, password, tableName)
        uniInfoLog("Expected Rows: " + str(rowsPXF), report)
        uniInfoLog("Actual Rows  : " + str(rows), report)
        if emailAddress:
            Email.sendEmail(emailAddress, "Completed Load of " + tableName, "Loaded " + str(rows) + " Rows")
    if (emailAddress):
        messageLines = []
        with open(reportName, "r") as reportMsg:
            for line in reportMsg.readlines():
                messageLines.append(line)
            message = " ".join(messageLines)
            Email.sendEmail(emailAddress, "Table Load Final Report: " + (reportName.split('/')[2])[:-4], message)


def analyzeHawqTables(master, database, username, password, emailAddress=""):
    loggerInfo = buildReportLogger("analyze")
    reportName = loggerInfo[0]
    report = loggerInfo[1]
    header = []
    startString = "Analyze Database Tables to Generate Statistics"
    uniInfoLog(startString, report)
    header = "Analyzing HAWQ Tables"
    uniInfoLog(header, report)

    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    with queries.Session(hawqURI) as session:
        uniInfoLog("Analyze Dimension Tables", report)
        for table in dimensionTables:
            ddlString = "Analyze " + table
            startTime = datetime.datetime.now()
            uniInfoLog("Start " + ddlString + ": " + str(startTime), report)
            result = session.query(ddlString)
            stopTime = datetime.datetime.now()
            resultString = "Completed " + ddlString + ": " + str(stopTime) + " Elapsed Time: " + str(
                stopTime - startTime)
            uniInfoLog(resultString, report)
            if emailAddress:
                Email.sendEmail(emailAddress, ddlString + " Complete", resultString)
        uniInfoLog("Analyze Fact Tables", report)

        for table in factTables:
            ddlString = "analyze " + table
            startTime = datetime.datetime.now()
            uniInfoLog("Start " + ddlString + ": " + str(startTime), report)
            result = session.query(ddlString)
            stopTime = datetime.datetime.now()
            resultString = "Completed " + ddlString + ": " + str(stopTime) + " Elapsed Time: " + str(
                stopTime - startTime)
            uniInfoLog(resultString, report)
            if emailAddress:
                Email.sendEmail(emailAddress, ddlString + " Complete", resultString)

        if (emailAddress):
            messageLines = []
            with open(reportName, "r") as reportMsg:
                for line in reportMsg.readlines():
                    messageLines.append(line)
                message = " ".join(messageLines)
                Email.sendEmail(emailAddress, "Table Analyze Final Report: " + (reportName.split('/')[2])[:-4], message)


def rowCount(master, database, username, password, table):
    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    with queries.Session(hawqURI) as session:
        result = session.query("select count(*) from " + table + ";")
        return str(result.items()[0]['count'])


def getDatabase(master, username, password):
    hawqURI = queries.uri(master, port=5432, dbname='gpadmin', user=username, password=password)
    try:
        dbName = input("Please Enter a Name for the HAWQ TPC-DS Database:")
        with queries.Session(hawqURI) as session:
            result = session.query("create database " + dbName)
    except psycopg2.ProgrammingError as e:
        print "Database already exists. "
    return dbName


def getPartitionCount(master, database, username, password, table):
    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    with queries.Session(hawqURI) as session:
        queryDDL = "SELECT count(*) AS partitions FROM   pg_inherits i WHERE  i.inhparent = '" + table + "'::regclass;"
        result = session.query(queryDDL)
        partitions = result.items()[0]["partitions"]

    return partitions


def partitionTables(master, parts, username, password, database, orientation, byPart, compressType, rowGroupSize,
                    emailAddress=""):
    loggerInfo = buildReportLogger("partitioning")
    reportName = loggerInfo[0]
    report = loggerInfo[1]
    startString = "Partitioning Tables into " + str(parts) + " Day Partitions in " + orientation + " Format"
    uniInfoLog(startString, report)
    if orientation.upper() == "PARQUET":
        # orientation = "PARQUET,ROWGROUPSIZE=1073741823,COMPRESSTYPE=snappy"
        orientation = "PARQUET,ROWGROUPSIZE=" + rowGroupSize + ",COMPRESSTYPE=" + compressType

    hawqURI = queries.uri(master, port=5432, dbname=database, user=username, password=password)
    if byPart:
        loadList = sorted(glob.glob('./hawq-ddl/load-partbypart/*.sql'))
    else:
        loadList = sorted(glob.glob('./hawq-ddl/load-part/*.sql'))

    tableList = sorted(glob.glob('./hawq-ddl/hawq-part/*.sql'))
    with queries.Session(hawqURI) as session:
        for table in tableList:
            ddlFile = open(table, "r")
            tableName = (table.split("/")[3]).split(".")[0]
            createStatus = "Creating Table: " + tableName
            uniInfoLog(createStatus, report)
            tableDDL = ddlFile.read()
            tableDDL = tableDDL.replace("$PARTS", parts)
            tableDDL = tableDDL.replace("$ORIENTATION", orientation)
            result = session.query(tableDDL)
            createStatus = "Table Created: " + tableName
            uniInfoLog(createStatus, report)
            if emailAddress:
                Email.sendEmail(emailAddress, createStatus, createStatus)


    # Hard Coded for now because Schema is HardCoded as well
    startDate = 2450815
    endDate = 2453005
    totalDays = endDate - startDate

    for load in loadList:
        ddlFile = open(load, "r")
        loadDDL = ddlFile.read()
        if byPart:
            tableName = ((load.split("/")[3]).split(".")[0])[:-13]
        else:
            tableName = ((load.split("/")[3]).split(".")[0])[:-5]
        loadStatus = "Loading: " + tableName
        uniInfoLog(loadStatus, report)
        ddlFile = open(load, "r")
        loadDDL = ddlFile.read()
        if byPart:
            partCount = getPartitionCount(master, database, username, password, "inventory")
            partStart = startDate

            for partNum in range(2, partCount + 1):
                modDDL = loadDDL
                # with queries.Session(hawqURI) as session:

                partName = tableName + "_1_prt_" + str(partNum)
                # End of part is num days in the part added to the first day
                partEnd = partStart + (int(parts) - 1)
                modDDL = modDDL.replace("$PARTNAME", str(partName))
                modDDL = modDDL.replace("$PARTVALUE1", str(partStart))
                modDDL = modDDL.replace("$PARTVALUE2", str(partEnd))

                with queries.Session(hawqURI) as session:
                    result = session.query(modDDL)
                partStart = partEnd + 1
                createStatus = "Table Partition Loaded: " + partName
                uniInfoLog(createStatus, report)
            createStatus = "Table Loaded: " + tableName
            uniInfoLog(createStatus, report)
            if emailAddress:
                Email.sendEmail(emailAddress, createStatus, createStatus)
                # alterniatve
                # SELECT partitionboundary, partitiontablename, partitionname, partitionlevel, partitionrank FROM pg_partitions WHERE tablename='catalog_returns';

        else:
            with queries.Session(hawqURI) as session:
                result = session.query(loadDDL)
            createStatus = "Table Loaded: " + tableName
            uniInfoLog(createStatus, report)
            if emailAddress:
                Email.sendEmail(emailAddress, createStatus, createStatus)

    if (emailAddress):
        messageLines = []
        with open(reportName, "r") as reportMsg:
            for line in reportMsg.readlines():
                messageLines.append(line)
            message = " ".join(messageLines)
            Email.sendEmail(emailAddress, "Repartition Final Report: " + (reportName.split('/')[2])[:-4], message)


def capacityReport(namenode, hdfsDir):
    results = Hadoop.size(hdfsDir)
    print results[1]


def getQuerySet(setName):
    try:
        with open("./querysets/" + setName + ".set", "r") as querySetFile:
            querySet = querySetFile.read()
            queryList = querySet.split(",")
    except IOError as e:
        if setName == "all":
            queryList = [0]
        else:
            print "ERROR: Set Not Found:  Plese use 'all' or a valid name"
            exit()

    return queryList


def main(args):
    username = "gpadmin"

    if (args.subparser_name == "load"):

        password = getGpadminCreds(args.hawqMaster)
        createTables(args.hawqMaster, args.database, username, password)
        createPXFTables(args.hawqMaster, args.database, username, password, args.scale, args.base, args.namenode)
        loadHawqTables(args.hawqMaster, username, password, args.database, args.emailAddress)
    elif (args.subparser_name == "gen"):
        generateData(args.scale, args.base, args.namenode)  # , args.tableName)
    elif (args.subparser_name == "query"):
        logging.info("Query")
        if (args.querySet):
            queryList = getQuerySet(args.querySet)
        elif (args.queryNum):
            queryList = (args.queryNum).split(',')
        else:
            print "Either --set <setname> or --num <csv list of queries> is Required"
            exit()
        password = getGpadminCreds(args.hawqMaster)

        adminPassword = getAdminCreds(args.hostsFile, args.adminUser)

        # explain=False
        # if args.explain:
        #     explain=True
#def executeQueries(master, database, username, password, queryList, hostsFile, explain,adminUser, adminPassword, scale,
#                   emailAddress=""):
        if (args.emailAddress):
            executeQueries(args.hawqMaster, args.database, username, password, queryList, args.hostsFile,args.explain,
                           args.adminUser,
                           adminPassword, args.scale,args.emailAddress)
        else:
            executeQueries(args.hawqMaster, args.database, username, password, queryList, args.hostsFile,args.explain,
                           args.adminUser,
                           adminPassword,args.scale)

    elif (args.subparser_name == "part"):

        password = getGpadminCreds(args.hawqMaster)
        if (args.orientation):
            orientation = args.orientation
            if (args.rowGroupSize):
                rowGroupSize = args.rowGroupSize
            else:
                rowGroupSize = "8192"
            if (args.compressType):
                compressType = "SNAPPY"
            else:
                compressType = "NONE"
        else:
            orientation = "ROW"
        if (args.bypart):
            byPart = args.bypart
        else:
            byPart = False

        if (args.emailAddress):
            partitionTables(args.hawqMaster, args.parts, username, password, args.database, orientation, byPart,
                            args.emailAddress)
        else:
            partitionTables(args.hawqMaster, args.parts, username, password, args.database, orientation, byPart)



    elif (args.subparser_name == "analyze"):

        password = getGpadminCreds(args.hawqMaster)
        if (args.emailAddress):
            analyzeHawqTables(args.hawqMaster, args.database, username, password, args.emailAddress)
        else:
            analyzeHawqTables(args.hawqMaster, args.database, username, password)

    elif (args.subparser_name == "report"):
        capacityReport(args.namenode, args.hdfsDir)


if __name__ == '__main__':
    cliParse()
