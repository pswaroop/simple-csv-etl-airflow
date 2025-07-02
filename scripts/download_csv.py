import requests

def download_csv():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    with open("/opt/airflow/files/raw.csv", "wb") as f:
        f.write(response.content)
    print("CSV downloaded successfully")

if __name__ == "__main__":
    download_csv()