"""
A simple script to fetch all the information of gmail messages
Author(s): tushar5353@gmail.com<Tushar Sharma>

Usage: del_gmail_mails.py
"""
import os.path
import pandas as pd
import mysql.connector

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main():
    """
    Driver Function

    * Creates the credentials - if not exist
    * Creates service
    * Gets DB connection
    * Fetch and writes the messages
    """
    creds = create_credentials()
    service = create_service(creds)
    labels = list_labels(service)
    connection = get_connection()
    write_messages(service, labels, connection)

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def main():
    """
    Driver Function

    * Creates the credentials - if not exist
    * Creates service
    * Gets DB connection
    * Fetch and writes the messages
    """
    creds = create_credentials()
    service = create_service(creds)
    labels = list_labels(service)
    connection = get_connection()
    write_messages(service, labels, connection)


def create_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def create_service(creds):
    """
    Creates gmail service

    :param creds: - google project credentials
    """
    service = build("gmail", "v1", credentials=creds)
    return service


def list_labels(service):
    """
    Returns the list of labels for ex INBOX, PROMOTIONS etc.

    :param service: - google service url
    """
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    if not labels:
        print("No labels found.")
        return
    print("Labels:")
    labels = [label["name"] for label in labels]
    print(labels)
    return labels


def write_messages(service, labels, connection):
    """
    Gets the message info and write it to csv files

    :param service: - gmail service
    :param labels: - list of gmail labels
    :param connection: - DB connection
    """
    info = []
    for label in labels:
        if label == "CHAT":
            print(f"Skipping Label :: {label}")
            continue
        print(f"{'##'*10}Processing for Label {label}{'##'*10}")
        get_message_info(service, label, connection)
        print(f"Processing Done for Label {label}")


def get_message_info(service, label, connection):
    """
    Gets the message info and write it to csv file label wise
    until last message

    :param service: - gmail service
    :param labels: - single gmail labels
    :param connection: - DB connection
    """
    message_ids_info = get_messages(service, label, page_id=0)
    next_page = (
        1
        if "nextPageToken" not in message_ids_info
        else message_ids_info["nextPageToken"]
    )
    count = 0
    cursor = connection.cursor(dictionary=True, buffered=True)
    while next_page:
        temp_list = []
        all_ids = _get_processed_message_ids(cursor)
        num = len(all_ids[label]) if label in all_ids else 0
        print(f"Already processed messages for Label {label} - {num}")
        print("Processing remaining messages")
        for i in message_ids_info["messages"]:
            info = _get_message_content(service, label, all_ids, i["id"], next_page)
            if not info:
                continue
            else:
                temp_list.append(info)
        count += 1
        if len(temp_list):
            file_name = f"mails_info/{label}_{count}.csv"
            _write_to_file(temp_list, file_name)
            inserts = _multi_inserts(temp_list)
            cursor.execute(inserts)
        else:
            print("-- Batch Already Processed --")
        if "nextPageToken" in message_ids_info:
            next_page = message_ids_info["nextPageToken"]
            message_ids_info = get_messages(service, label, page_id=next_page)
            print(len(message_ids_info["messages"]), next_page)
        else:
            next_page = None
    return temp_list


def get_messages(service, label, page_id):
    """
    Gets the message info and write it to a csv file for a page_id

    :param service: - gmail service
    :param label: - single gmail label
    :param page_id: - unique page_id(uuid)
    """
    print("--"*40)
    print(f"Getting messages for Page Id :: {page_id}")
    if page_id == 0:
        return (
            service.users()
            .messages()
            .list(userId="me", labelIds=[label], maxResults=500)
            .execute()
        )
    else:
        return (
            service.users()
            .messages()
            .list(userId="me", labelIds=[label], pageToken=page_id, maxResults=500)
            .execute()
        )


def _get_processed_message_ids(cursor):
    """
    Gets already processed message_ids

    :param cursor: - DB cursor
    """
    print(f"Getting Already processed message_ids")
    info = {}
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    query = f"""SELECT label,
                        message_id
                FROM    mysql_mails.message_ids"""
    cursor.execute(query)
    rows = cursor.fetchall()
    if not len(rows):
        return {}
    for row in rows:
        if row["label"] in info:
            info[row["label"]].append(row["message_id"])
        else:
            info[row["label"]] = [row["message_id"]]
    return info


def _get_message_content(service, label, all_ids, message_id, next_page):
    """
    Gets the message content for the messages which are not processed

    :param service: - gmail service
    :param label: - single gmail label
    :param all_ids: - list of message_ids which are processed
    :param message_id: - unique message_id
    :param next_page: - page id of next page
    """
    info = {"sender": None, "receiver": None, "date": None, "subject": None}
    process_status = _processed_status(all_ids, message_id, label)
    if process_status == "different_label":
        info["message_id"] = message_id
        info["label"] = label
        info["next_page_id"] = next_page
    elif process_status == "not_processed":
        info = service.users().messages().get(userId="me", id=message_id).execute()
        header = info["payload"]["headers"]
        info = _get_each_message_info(service, message_id)
        info["message_id"] = message_id
        info["label"] = label
        info["next_page_id"] = next_page
    else:
        return None
    return info


def _processed_status(all_ids, message_id, label):
    """
    Returns the message status if it is processed or not

    :param all_ids: - list of all message ids which are processed
    :param message_id: - message id to check against all_ids
    :param page_id: - single label
    """
    label_present = True if label in all_ids.keys() else False
    if label_present and message_id in all_ids[label]:
        return "same_label"
    for key, value in all_ids.items():
        if message_id in value:
            return "different_label"
    return "not_processed"


def _get_each_message_info(service, message_id):
    """
    Gets the required message information
    Sender|date|subject|receiver

    :param service: - gmail service
    :param message_id: - unique message id
    """
    temp = {}
    info = service.users().messages().get(userId="me", id=message_id).execute()
    header = info["payload"]["headers"]
    for info in header:
        if info["name"] == "From":
            temp["sender"] = info["value"]
        if info["name"] == "Date":
            temp["date"] = info["value"]
        if info["name"] == "Subject":
            temp["subject"] = info["value"]
        if info["name"] == "To":
            temp["receiver"] = info["value"]
    return temp


def _write_to_file(content, file_name):
    """
    write the content of a message to a file

    :param content: - content of a message
    :param file_name: - CSV file name with Label
    """
    print(f"Writing to file :: {file_name}")
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        df2 = pd.DataFrame(content)
        final = pd.concat([df, df2])
        final.to_csv(file_name, index=False)
    else:
        df = pd.DataFrame(content)
        df.to_csv(file_name, index=False)


def _multi_inserts(info):
    """
    Function to create multi insert statement to speed up the insert operation
    This is basically for maintaining the metadata

    """
    common_string = "INSERT INTO mysql_mails.message_ids(message_id, label) values"
    temp = ""
    for i in info:
        temp += f"('{i['message_id']}', '{i['label']}'),"
    temp = temp.strip(",")
    final_insert = common_string + temp
    return final_insert


def get_connection():
    """
    returns the DB connection
    """
    connection = mysql.connector.connect(
        host="localhost", user="mysql_mails", passwd="password", autocommit=True
    )
    return connection


if __name__ == "__main__":
    main()
