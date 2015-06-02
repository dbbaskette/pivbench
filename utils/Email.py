import smtplib
import datetime



def sendEmail(address, message):



    fromaddr = 'pivbench@gmail.com'
    toaddrs = address
    subject = "PivBench Status Report " + str(datetime.datetime.now())
    message = 'Subject: %s\n\n%s' % (subject, message)

    username = 'pivbench'
    userinfo = 'uuhmdtqvheskxnno'

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, userinfo)
    server.sendmail(fromaddr, toaddrs, message)
    server.quit()