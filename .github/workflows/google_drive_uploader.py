import os, json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

CREDS = json.loads(os.getenv('GOOGLE_DRIVE_CREDENTIALS'))
FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')

creds = service_account.Credentials.from_service_account_info(CREDS, scopes=['https://www.googleapis.com/auth/drive.file'])
service = build('drive', 'v3', credentials=creds)

for fname in ["Trading_Journal.xlsx", "swing_candidates.xlsx"]:
    if not os.path.exists(fname): continue
    
    # Find existing file
    query = f"name='{fname}' and '{FOLDER_ID}' in parents"
    results = service.files().list(q=query).execute().get('files', [])
    
    media = MediaFileUpload(fname, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    if results:
        service.files().update(fileId=results[0]['id'], media_body=media).execute()
        print(f"✅ Updated {fname}")
    else:
        service.files().create(body={'name': fname, 'parents': [FOLDER_ID]}, media_body=media).execute()
        print(f"✅ Uploaded {fname}")