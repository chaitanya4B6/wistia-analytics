import json
import boto3
import requests
from datetime import datetime

# --- CONFIG ---
S3_BUCKET = "wistia-project-raw"  # replace with your bucket
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

# Step 1: read secret from Secrets Manager
def get_wistia_token():
    client = boto3.client("secretsmanager", region_name="us-east-1")
    secret = client.get_secret_value(SecretId="wistia/api-token")
    return json.loads(secret["SecretString"])["wistia_api_token"]

# Step 2: fetch data for each media
def fetch_wistia_stats(media_id, token):
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# Step 3: upload raw JSON to S3
def upload_to_s3(content, media_id):
    s3 = boto3.client("s3")
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{media_id}/{now}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(content))
    print("Uploaded:", f"s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    token = get_wistia_token()
    print("Secret retrieved from Secrets Manager!")

    for m in MEDIA_IDS:
        print("Fetching data for:", m)
        stats = fetch_wistia_stats(m, token)
        upload_to_s3(stats, m)
