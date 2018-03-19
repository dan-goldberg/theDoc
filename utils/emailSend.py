
# coding: utf-8

# In[2]:

def emailSend(msg='test',
               subject='test',
               fromaddr='dgoldberg.autoemails@gmail.com',
               toaddrs='dgoldberg48@gmail.com',
               username='dgoldberg.autoemails@gmail.com',
               password='Gmail1Gberg99',
               servername='smtp.gmail.com:587'
               ):
    
    messege = "\r\n".join([
    "From: "+fromaddr,
    "To: "+toaddrs,
    "Subject: "+subject,
    "",
    msg
    ])
    
    import smtplib
    server = smtplib.SMTP(servername)
    server.ehlo()
    server.starttls()
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, messege)
    server.quit()




