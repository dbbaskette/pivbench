import smtplib
import datetime

from passlib.apps import custom_app_context as pwd_context


def writepass():
    hash = pwd_context.encrypt("uuhmdtqvheskxnno")
    with open("./utils/.passfile", "w") as passFile:
        passFile.write(hash)


def sendEmail(address, message):
    # writepass()


    fromaddr = 'pivbench@gmail.com'
    toaddrs = address
    subject = "PivBench Status Report " + str(datetime.datetime.now())
    message = 'Subject: %s\n\n%s' % (subject, message)

    # Credentials (if needed)
    username = 'pivbench'
    with open("./utils/.passfile", "r") as passFile:
        password =
    #password = 'uuhmdtqvheskxnno'

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, "")
    server.sendmail(fromaddr, toaddrs, message)
    server.s
    server.quit()