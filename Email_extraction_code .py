#!/usr/bin/env python
# coding: utf-8

# In[1]:


#### extract and insert in db 


import datetime
# from dateutil import parser
# import re
import uuid
import email
import imaplib
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector

db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'vrdella!6',
    'database': 'email_extraction'
}


def get_unread_emails(email_address, password, label):
    imap_server = "imap.gmail.com"
    imap_port = 993

    imap_conn = imaplib.IMAP4_SSL(imap_server, imap_port)

    imap_conn.login(email_address, password)

    labels = label
    imap_conn.select(labels)

    _, message_ids = imap_conn.search(None, "(UNSEEN)")

    unread_emails = []

    for message_id in message_ids[0].split():
        try:
            _, data = imap_conn.fetch(message_id, "(RFC822)")
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)

            subject = email_message["Subject"]
            sender = email.utils.parseaddr(email_message["From"])[1]
            body1 = ""

            # Extract the "Date" header to get the receiving time
            date_received = parsedate_to_datetime(email_message["Date"])

            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_type() == "text/plain":
                        body1 = part.get_payload(decode=True).decode()
                    elif part.get_content_type() == "text/html":
                        body1 = html2text.html2text(part.get_payload(decode=True).decode())
            else:
                body1 = email_message.get_payload(decode=True).decode()

            email_data = {
                "subject": subject,
                "sender": sender,
                "body": body1,
                "date_received": date_received
            }

            unread_emails.append(email_data)
        except Exception as e:
            print(f"Error processing email: {str(e)}")

            imap_conn.store(message_id, '-FLAGS', '(\Seen)')

    imap_conn.close()
    return unread_emails


class client_mail_extraction:

    def insert_data_into_mysql(self, result_data):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            cursor = conn.cursor()

            for result_data_item in result_data:
                cursor.execute(
                    "SELECT COUNT(*) FROM client_data WHERE client_job_id = %s",
                    (result_data_item['client_job_id'],)
                )
                record_count = cursor.fetchone()[0]

                if record_count == 0:
                    update_query = """
                    INSERT INTO client_data(
                        job_title,
                        client_job_id,
                        client_name,
                        client_id,
                        job_status,
                        job_id
                    ) VALUES (
                        %(job_title)s,
                        %(client_job_id)s,
                        %(client_name)s,
                        %(client_id)s,
                        %(job_status)s,
                        %(job_id)s

                    )
                    """
                else:
                    update_query = """
                    UPDATE client_data
                    SET
                        job_title = %(job_title)s,
                        client_name = %(client_name)s,
                        client_id = %(client_id)s,
                        job_status=%(job_status)s,
                        job_id=%(job_id)s
                    WHERE client_job_id = %(client_job_id)s
                    """

                data_to_insert = {
                    "job_title": result_data_item['job_title'],
                    "client_job_id": result_data_item['client_job_id'],
                    "client_name": result_data_item['client_name'],
                    "client_id": result_data_item['client_id'],
                    "job_status": result_data_item['job_status'],
                    "job_id": str(uuid.uuid4())}

                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(update_query, data_to_insert)

            conn.commit()
            return "Data inserted/updated successfully"
        except mysql.connector.Error as error:
            print(f"Error inserting/updating data into MySQL: {error}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def extract_client_details(self):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT client_id,email_id,password,client_name "
                               "FROM client WHERE client_name = 'Atlanta1'")
                result = cursor.fetchone()

                if result:
                    return result
                else:
                    return None

        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")

        finally:
            if conn.is_connected():
                conn.close()

    def email_data_information(self):

        labels = "Atlanta1"
        vms_data = self.extract_client_details()
        unread_emails = get_unread_emails(vms_data[1], vms_data[2], labels)

        extracted_results = []
        date_received = ''
        result = {}

        for email_data in unread_emails:
            result = {}
            date_received = email_data['date_received'].strftime("%Y-%m-%d %H:%M:%S")
            email_body = email_data['body']
            lines = email_body.split("\n")
            for line in lines:
                if line.startswith("3\."):
                    result["client_job_id"] = line.split("-", 1)[1].replace(" ",'')
                elif line.startswith("Network"):
                    result["job_title"] = line.replace(".", '')
            result['job_status'] = "pending"

        if all(result.values()):
            extracted_results.append(result)

        formatted_results = []
        for result in extracted_results:
            formatted_result = {
                "client_job_id": result.get("client_job_id", ''),
                "job_title": result.get("job_title", ''),
                "job_status": result.get("job_status", ''),
                "client_id": vms_data[0],
                "client_name": vms_data[3]

            }
            formatted_results.append(formatted_result)
        print(formatted_results)
        result = self.insert_data_into_mysql(formatted_results)
        print("Insert Result:", result)
        return result



my_instance = client_mail_extraction()

insertion_result = my_instance.email_data_information()




# In[2]:


##### extract and update in db 


import datetime
# from dateutil import parser
# import re
import uuid
import email
import imaplib
from email.utils import parsedate_to_datetime
import html2text
import mysql.connector

db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'vrdella!6',
    'database': 'email_extraction'
}


def get_unread_emails(email_address, password, label):
    imap_server = "imap.gmail.com"
    imap_port = 993

    imap_conn = imaplib.IMAP4_SSL(imap_server, imap_port)

    imap_conn.login(email_address, password)

    labels = label
    imap_conn.select(labels)

    _, message_ids = imap_conn.search(None, "(UNSEEN)")

    unread_emails = []

    for message_id in message_ids[0].split():
        try:
            _, data = imap_conn.fetch(message_id, "(RFC822)")
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)

            subject = email_message["Subject"]
            sender = email.utils.parseaddr(email_message["From"])[1]
            body1 = ""

            # Extract the "Date" header to get the receiving time
            date_received = parsedate_to_datetime(email_message["Date"])

            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_type() == "text/plain":
                        body1 = part.get_payload(decode=True).decode()
                    elif part.get_content_type() == "text/html":
                        body1 = html2text.html2text(part.get_payload(decode=True).decode())
            else:
                body1 = email_message.get_payload(decode=True).decode()

            email_data = {
                "subject": subject,
                "sender": sender,
                "body": body1,
                "date_received": date_received
            }

            unread_emails.append(email_data)
        except Exception as e:
            print(f"Error processing email: {str(e)}")

            imap_conn.store(message_id, '-FLAGS', '(\Seen)')

    imap_conn.close()
    return unread_emails


class client_mail_extraction:

    def insert_data_into_mysql(self, result_data):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            cursor = conn.cursor()

            for result_data_item in result_data:
                cursor.execute(
                    "SELECT COUNT(*) FROM client_data WHERE client_job_id = %s",
                    (result_data_item['client_job_id'],)
                )
                record_count = cursor.fetchone()[0]

                if record_count == 0:

                    update_query = """
                    INSERT INTO client_data(
                        job_title,
                        client_job_id,
                        client_name,
                        client_id,
                        job_status,
                        comment,
                        job_id
                    ) VALUES (
                        %(job_title)s,
                        %(client_job_id)s,
                        %(client_name)s,
                        %(client_id)s,
                        %(job_status)s,
                        %(comment)s,
                        %(job_id)s
                    )
                    """
                else:

                    update_query = """
                    UPDATE client_data
                    SET
                        job_title = %(job_title)s,
                        client_name = %(client_name)s,
                        client_id = %(client_id)s,
                        job_status = %(job_status)s,
                        comment = %(comment)s
                    WHERE client_job_id = %(client_job_id)s
                    """

                data_to_insert = {
                    "job_title": result_data_item['job_title'],
                    "client_job_id": result_data_item['client_job_id'],
                    "client_name": result_data_item['client_name'],
                    "client_id": result_data_item['client_id'],
                    "job_status": result_data_item['job_status'],
                    "comment": result_data_item['comment'],
                    "job_id": str(uuid.uuid4())
                }

                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(update_query, data_to_insert)

            conn.commit()
            return "Data inserted/updated successfully"
        except mysql.connector.Error as error:
            print(f"Error inserting/updating data into MySQL: {error}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def extract_client_details(self):
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT client_id,email_id,password,client_name "
                               "FROM client WHERE client_name = 'Atlanta1'")
                result = cursor.fetchone()

                if result:
                    return result
                else:
                    return None

        except mysql.connector.Error as error:
            print(f"Error connecting to MySQL: {error}")

        finally:
            if conn.is_connected():
                conn.close()

    def email_data_information(self):

        labels = "Atlanta1"
        vms_data = self.extract_client_details()
        unread_emails = get_unread_emails(vms_data[1], vms_data[2], labels)

        extracted_results = []
        date_received = ''
        result = {}

        for email_data in unread_emails:
            date_received = email_data['date_received'].strftime("%Y-%m-%d %H:%M:%S")
            email_subject = email_data['subject']
            lines = email_subject.split("\n")
            for line in lines:
                if "Requisition" in line:
                    result["client_job_id"] = line.split("Requisition", 1)[1].split("is", 1)[0].replace(" ", '')

            email_body = email_data["body"]
            lines1 = email_body.split("\n")
            for line1 in lines1:
                if line1.startswith("Requisition Name/ID:"):
                    result["job_title"] = line1.split("Requisition Name/ID:", 1)[1].split("/", 1)[0]
                elif line1.startswith("Information"):
                    result["comment"] = line1
            result["job_status"] = "not accepted"

        if all(result.values()):
            extracted_results.append(result)

        formatted_results = []
        for result in extracted_results:
            formatted_result = {
                'client_job_id': result.get("client_job_id", ""),
                'job_title': result.get("job_title", ''),
                'comment': result.get("comment", ''),
                'job_status': result.get("job_status", ''),
                'client_id': vms_data[0],
                'client_name': vms_data[3]
            }
            formatted_results.append(formatted_result)

        print(formatted_results)

        result = self.insert_data_into_mysql(formatted_results)
        print("Insert Result:", result)
        return result


my_instance = client_mail_extraction()

insertion_result = my_instance.email_data_information()




# In[ ]:





# In[ ]:




