from prefect import flow, task
import pandas as pd
import requests
from prefect.artifacts import create_markdown_artifact

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
    df.to_json("projects.json", index=False)
    path = "/tmp/projects.json"
    df.to_json(path, orient="records", force_ascii=False)
    print(f"✅ Saved to {path}")

    # Optional: attach as artifact so you can download from the UI
    create_markdown_artifact(f"Project list saved to `{path}` with {len(df)} records.")

@flow(name="fetch-inmuebles")
def main_flow():
    data = fetch_data()
    save_to_json(data)

if __name__ == "__main__":
    main_flow()
