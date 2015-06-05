__author__ = 'dbbaskette'

import json

import sh
import requests


def ls(hdfsPath):
    try:
        return (0,sh.hadoop("fs","-ls",hdfsPath))
    except sh.ErrorReturnCode as e:
        return (-1,e.stderr)

def mkdir(hdfsPath):
    try:
        return (0,sh.hadoop("fs","-mkdir","-p",hdfsPath))
    except sh.ErrorReturnCode as e:
        return (-1,e.stderr)

def run(jarFile,scale,base):
    try:
        return (0,sh.hadoop("jar",jarFile,"-d",base+"/"+str(scale)+"/","-s",scale))
    except sh.ErrorReturnCode as e:
        print e
        return (-1,e.stderr)


def runTable(jarFile, scale, base, tableName):
    try:
        return (0, sh.hadoop("jar", jarFile, "-d", base + "/" + str(scale) + "/", "-s", scale, "-t", tableName))
    except sh.ErrorReturnCode as e:
        print e
        return (-1, e.stderr)

def setrep(repFactor,dirName):
    try:
        return (0,sh.hadoop("fs","-setrep","-R",repFactor,dirName))
    except sh.ErrorReturnCode as e:
        print e
        return (-1,e.stderr)

def size(hdfsPath):
    try:
        return (0,sh.hadoop("fs","-du","-h",hdfsPath))
    except sh.ErrorReturnCode as e:
        print e
        return (-1,e.stderr)


def appStatus(resourceManager):
    url = "http://" + resourceManager + ":8088/ws/v1/cluster/apps"
    response = requests.get(url)
    appsJSON = json.loads(response.text)
    print appsJSON

    exit()

# cd tpcds-gen; hadoop jar target/*.jar -d ${DIR}/${SCALE}/ -s ${SCALE})
# url = "http://" + hostName + ":8080/api/v1/stacks/PHD/versions/3.0/operating_systems/redhat6/repositories/PHD-3.0"
# requests.put(url, auth=auth, headers=headers, data=payload)



