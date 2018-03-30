import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from theDoc import settings

def emailSend(msg='test',
               subject='test',
               fromaddr = settings.EMAIL_FROM,
               toaddrs = settings.EMAIL_TO,
               username = settings.EMAIL_FROM,
               password = settings.EMAIL_PWD,
               servername = settings.EMAIL_SERVER
               ):
    
    messege = "\r\n".join([
    "From: "+fromaddr,
    "To: "+toaddrs,
    "Subject: "+subject,
    "",
    msg
    ])
    
    server = smtplib.SMTP(servername)
    server.ehlo()
    server.starttls()
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, messege)
    server.quit()


def htmlEmailSend(
               html=None,
               subject='test',
               fromaddr = settings.EMAIL_FROM,
               toaddrs = settings.EMAIL_TO,
               username = settings.EMAIL_FROM,
               password = settings.EMAIL_PWD,
               servername = settings.EMAIL_SERVER
               ):
    

    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = fromaddr
    message['To'] = toaddrs
    htmlpart = MIMEText(html, 'html')
    message.attach(htmlpart)
    
    server = smtplib.SMTP(servername)
    server.ehlo()
    server.starttls()
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, message.as_string())
    server.quit()
