import smtplib
import datetime



def sendEmail(address, subject, message):



    fromaddr = 'pivbench@gmail.com'
    toaddrs = address
    message = 'Subject: %s\n\n%s' % (subject, message)

    username = 'pivbench'
    userinfo = 'uuhmdtqvheskxnno'
    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, userinfo)
    server.sendmail(fromaddr, toaddrs, message)
    server.quit()