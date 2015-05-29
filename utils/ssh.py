# coding: utf-8

# Copyright (C) 2013-2014 Pivotal Software, Inc.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the under the Apache License,
# Version 2.0 (the "License”); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import paramiko, socket, time,os,logging



# Deprecated
def exec_command(host, username, password, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # TODO wait until sshd runs
    ssh.connect(host, username=username, password=password,look_for_keys=False)
    # ssh.exec_command(command )
    # session = ssh._transport().open_session()

    stdin, stdout, stderr = ssh.exec_command(cmd)
    # return_code = session.c.recv_exit_status()
    return_code = ssh.c.recv_exit_status()

    stdoutString = stdout.read()
    stderrString = stderr.read()
    ssh.close()
    return return_code, stdoutString, stderrString

def exec_command2(host, username, password, cmd, inputs=None):
    logger = logging.getLogger("Root Logger")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # TODO wait until sshd runs

    ssh.connect(host, username=username, password=password,look_for_keys=False)
    # ssh.exec_command(command )
    # session = ssh._transport().open_session()

    stdin, stdout, stderr = ssh.exec_command(cmd)
    # return_code = session.c.recv_exit_status()
    if(inputs is not None and type(inputs) is list):
        for ainput in inputs:
            stdin.write(ainput + "\n")
        stdin.flush()

    return_code = None

    stdoutString = stdout.read()
    stderrString = stderr.read()
    ssh.close()
    return return_code, stdoutString, stderrString

RETRY_COUNT = 20

def connection_check(hostname, user, password):
    count = 0
    while count < RETRY_COUNT:
        count += 1
        try:
            exec_command2(hostname, user, password, "ls /")
            return True
        except socket.error:
            print "."

        time.sleep(1)

    return False

#DBB ADD
def copyKey(key,hostname,username,password):

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)
    sftp = ssh.open_sftp()
    usernamePath=username
    if (username!="root"):
        usernamePath = "home/"+username
    pubKeyFile = sftp.open("/"+usernamePath+"/.ssh/authorized_keys","a")
    pubKeyFile.write(key)
    pubKeyFile.close()
    exec_command2(hostname,username,password,"chmod 600 /"+usernamePath+"/.ssh/authorized_keys")


#DBB ADD
def getKey(hostname,username,password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # TODO wait until sshd runs
    ssh.connect(hostname, username=username, password=password)
    sftp = ssh.open_sftp()
    if (username!="root"):
        username = "home/"+username
    pubKeyFile = sftp.open("/"+username+"/.ssh/id_rsa.pub")
    pubKey = pubKeyFile.read()
    pubKeyFile.close()
    return pubKey

#DBB ADD
def putFile(hostname,filePath,username,password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # TODO wait until sshd runs
    ssh.connect(hostname, username=username, password=password)
    sftp = ssh.open_sftp()
    path,filename = os.path.split(filePath)
    sftp.put(filePath,"/tmp/"+filename)
    sftp.close()
    ssh.close()
    return filename

def getFile(hostname,remoteFile,localFile,username,password):
    transport=paramiko.Transport(hostname)
    transport.connect(None,username,password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    #sftp.open()
    #sftp.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # TODO wait until sshd runs
    #sftp.connect(hostname, username=username, password=password)
    sftp.get(remoteFile,localFile)
    sftp.close()
    return 0


if __name__ == "__main__" :
    retcode, stdout, stderr = exec_command2(
                                    "172.17.0.2"
                                    , "root"
                                    , "changeme"
                                    , "passwd aaa", ["aaa", "aaa"])
    print "retcode: " + str(retcode)
    print "stdout: " + stdout
    print "stderr: " + stderr