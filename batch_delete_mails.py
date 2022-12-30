import os

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly",
          "https://www.googleapis.com/auth/gmail.modify",
          "https://mail.google.com/"]
#info = service.users().messages().batchDelete(userId="me", id=message_id).execute()
def create_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("/home/token.json"):
        creds = Credentials.from_authorized_user_file("/home/token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("/home/credentials.json", SCOPES)
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


creds = create_credentials()
service = create_service(creds)
x = service.users().messages().batchDelete(userId='me', body={"ids":["message_id1", "message_id2"]}).execute()
