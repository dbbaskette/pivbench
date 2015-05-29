__author__ = 'dbbaskette'

import sh

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

# cd tpcds-gen; hadoop jar target/*.jar -d ${DIR}/${SCALE}/ -s ${SCALE})
