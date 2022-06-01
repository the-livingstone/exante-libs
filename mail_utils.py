#!/usr/bin/env python3

import smtplib
import socket
from magic import Magic
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

default_sender = 'Robot <robot@' + socket.getfqdn() + '>'


def send_mail(mail_to, mail_from, subj, html, attachment=None):
    """
    sends email using local mail server
    :param subj: mail subject
    :param mail_to: either str with recepients address or list of them
    :param mail_from: sender email
    :param html: email text as plain text or html
    :param attachment: str with file location, or file-like object,
                       or list of them
    """
    # convert mail_to and attachment to list if string is given
    if isinstance(mail_to, str):
        mail_to = [mail_to]
    if isinstance(attachment, str) or isinstance(attachment, bytes):
        attachment = [attachment]

    # create message container - the correct MIME type is multipart/related
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subj
    msgRoot['From'] = mail_from
    msgRoot['To'] = ' ,'.join(mail_to)

    # create the body of the message (a plain-text and an HTML version)
    # record the MIME types of both parts - text/plain and text/html
    if html.startswith('<html>'):
        part1 = MIMEText('This email\'s content is HTML.', 'plain')
        part2 = MIMEText(html.encode('utf-8'), 'html', 'utf-8')
    else:
        part1 = MIMEText(html.encode('utf-8'), 'plain', 'utf-8')
        part2 = None

    # attach parts into message container
    # according to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msgAlternative = MIMEMultipart('alternative')
    msgAlternative.attach(part1)
    if part2 is not None:
        msgAlternative.attach(part2)

    msgRoot.attach(msgAlternative)

    mimetypes = Magic(mime=True)

    # create an attachment if any
    if attachment:
        for number, file_to_attach in enumerate(attachment):
            if isinstance(file_to_attach, str):
                with open(file_to_attach, 'rb') as file_stream:
                    reader = file_stream.read()
                file_name = basename(file_to_attach)
            elif isinstance(file_to_attach, bytes):
                reader = file_to_attach
                file_name = f'attachment{number}'
            else:
                raise Exception(f'Unknown type {type(file_to_attach)}')
            mime = mimetypes.from_buffer(reader)
            if mime is None:
                mime = 'octet-stream'
            else:
                mime = mime.split('/')[1]
            partA = MIMEApplication(reader, mime, Name=file_name)
            partA['Content-Disposition'] = 'attachment; filename="%s"' % file_name
            msgRoot.attach(partA)

    s = smtplib.SMTP('localhost')
    s.sendmail(mail_from, mail_to, msgRoot.as_string())
    s.quit()


def make_table(data, headers: list = list(), add_header=True):
    """
    makes html table from list of dictionaries
    :param data: table data
    :param headers: table headers used to sort and rewrite table headers
    Format: ['DicrionaryKey:TableHeader', ...]. First list element will be in
    firs table column and so on
    :param add_header: add first and end messages
    :return: html table
    """
    if not headers:
        headers = data[0].keys()
    html = '<html><header></header><body>' if add_header else ''

    html += '<table style=\'border: 1px solid black;border-collapse: collapse;\'><tr>'
    for header in headers:
        if ':' in header:
            html += '<th style=\'border: 1px solid black; background-color: \
              gray; color: white; padding: 5px;\'>{}</th>'.format(header.split(':')[1])
        else:
            html += '<th style=\'border: 1px solid black; background-color: \
              gray; color: white; padding: 5px;\'>{}</th>'.format(header)
    for row in data:
        html += '<tr>'
        for header in headers:
            for key in row.keys():
                if ':' in header:
                    check_header = header.split(':')[0]
                else:
                    check_header = header
                if key == check_header:
                    html += '<td style=\'border: 1px solid black; text-align: \
                      left; padding: 10px;\'>{}</td>'.format(row[key])

        html += '</tr>'
    html += '</table>'
    if add_header:
        html += '</body></html>'

    return html
