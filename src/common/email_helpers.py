import json
import os
import smtplib

from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv
load_dotenv()
DATASPOT_EMAIL_RECEIVERS = json.loads(os.getenv('DATASPOT_EMAIL_RECEIVERS'))
DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY = json.loads(os.getenv('DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY'))
DATASPOT_EMAIL_SERVER = os.getenv('DATASPOT_EMAIL_SERVER')
DATASPOT_EMAIL_SENDER = os.getenv('DATASPOT_EMAIL_SENDER')

if (not DATASPOT_EMAIL_RECEIVERS
        or not DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY
        or not DATASPOT_EMAIL_SERVER
        or not DATASPOT_EMAIL_SENDER):
    raise ValueError("DATASPOT_EMAIL_RECEIVERS, DATASPOT_EMAIL_SERVER, and DATASPOT_EMAIL_SENDER must be set in the environment variables")


def create_email_msg(subject="Python Notification", text="", img=None, attachment=None) -> MIMEMultipart:
    """
    Create an email message with optional attachments and images.
    
    Args:
        subject: Email subject line
        text: Email body text content
        img: Image file path or list of image file paths to attach
        attachment: File path or list of file paths to attach
        
    Returns:
        MIMEMultipart: Email message object ready to be sent
    """
    # build message contents
    msg = MIMEMultipart()
    msg['Subject'] = subject  # add in the subject
    msg.attach(MIMEText(text, 'plain', 'utf-8'))  # add plain text contents

    # check if we have anything given in the img parameter
    if img is not None:
        # if we do, we want to iterate through the images, so let's check that
        # what we have is actually a list
        if type(img) is not list:
            img = [img]  # if it isn't a list, make it one
        # now iterate through our list
        for one_img in img:
            img_data = open(one_img, 'rb').read()  # read the image binary data
            # attach the image data to MIMEMultipart using MIMEImage, we add
            # the given filename use os.basename
            msg.attach(MIMEImage(img_data, name=os.path.basename(one_img)))

    # we do the same for attachments as we did for images
    if attachment is not None:
        if type(attachment) is not list:
            attachment = [attachment]  # if it isn't a list, make it one

        for one_attachment in attachment:
            with open(one_attachment, 'rb') as f:
                # read in the attachment using MIMEApplication
                file = MIMEApplication(
                    f.read(),
                    name=os.path.basename(one_attachment)
                )
            # here we edit the attached file metadata
            file['Content-Disposition'] = f'attachment; filename="{os.path.basename(one_attachment)}"'
            msg.attach(file)  # finally, add the attachment to our message object
    return msg

def send_email(msg, technical_only: bool = False):
    """
    Send an email message using the configured SMTP server.
    
    Args:
        msg: Email message object to send
        technical_only: Will only send the mail to the technical admin and not the TeamMailBox if True. Default is False
    """
    # initialize connection to email server
    host = DATASPOT_EMAIL_SERVER
    smtp = smtplib.SMTP(host)

    # send email
    if technical_only:
        smtp.sendmail(from_addr=DATASPOT_EMAIL_SENDER,
                      to_addrs=DATASPOT_EMAIL_RECEIVERS_TECHNICAL_ONLY,
                      msg=msg.as_string())
    else:
        smtp.sendmail(from_addr=DATASPOT_EMAIL_SENDER,
                      to_addrs=DATASPOT_EMAIL_RECEIVERS,
                      msg=msg.as_string())
    smtp.quit()