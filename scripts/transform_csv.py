import pandas as pd

def transform():
    df = pd.read_csv(
        "/opt/airflow/files/raw.csv",
        quotechar='"',
        skipinitialspace=True
    )

    df.columns = (
        df.columns
          .str.strip()
          .str.replace('"', '', regex=False)
    )
    df['Height(Inches)'] = df['Height(Inches)'].astype(float)
    df['Weight(Pounds)'] = df['Weight(Pounds)'].astype(float)
    df.to_csv("/opt/airflow/files/transformed.csv", index=False)
    print("CSV transformed successfully")

if __name__ == "__main__":
    transform()