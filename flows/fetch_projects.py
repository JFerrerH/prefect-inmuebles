from prefect import flow, task
import pandas as pd
import requests
from prefect.artifacts import create_markdown_artifact
from google.cloud import storage
from google.oauth2.credentials import Credentials
from prefect.blocks.system import Secret

@task
def fetch_data():
    url = "https://new.informeinmobiliario.com/api/proyecto"
    params = {
        "concepto": 3,
        "order": "rand",
        "ciudad_proyecto": "",
        "barrio_proyecto": "",
        "pag": 1,
        "results_per_page": 9
    }
    headers = {
        "Authorization": "Basic aW5mb3JtZTppbmZvcm1lMjAyMUA=",
        "Accept": "application/json",
        "Origin": "https://www.informeinmobiliario.com",
        "Referer": "https://www.informeinmobiliario.com/"
    }

    all_projects = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        projects = data.get("response", [])
        if not projects:
            break
        all_projects.extend(projects)
        params["pag"] += 1

    return all_projects

@task
def save_to_json(projects):
    df = pd.DataFrame(projects)
    path = "/tmp/projects.json"
    df.to_json(path, orient="records", force_ascii=False)
    print(f"✅ Saved to {path}")
    create_markdown_artifact(f"Project list saved to `{path}` with {len(df)} records.")

@task
def upload_to_gcs(projects):
    # Save to file
    df = pd.DataFrame(projects)
    path = "/tmp/projects.json"
    df.to_json(path, orient="records", force_ascii=False)

    # Load token from Secret block
    token_block = Secret.load("gdrive-user-token")
    creds = Credentials.from_authorized_user_info(token_block.get())

    # Upload to GCS
    client = storage.Client(credentials=creds)
    bucket = client.bucket("new-projects-datalake")
    blob = bucket.blob("projects/projects.json")
    blob.upload_from_filename(path)

    print(f"✅ Uploaded to GCS: gs://new-projects-datalake/projects/projects.json")

@flow(name="fetch-inmuebles")
def main_flow():
    data = fetch_data()
    save_to_json(data)
    upload_to_gcs(data)

if __name__ == "__main__":
    main_flow()
