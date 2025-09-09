import pandas as pd

def load_raw(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    return df
