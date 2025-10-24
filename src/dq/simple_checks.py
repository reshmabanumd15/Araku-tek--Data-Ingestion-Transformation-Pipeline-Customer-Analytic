from pathlib import Path
import pandas as pd

def check_no_null_emails(df: pd.DataFrame) -> bool:
    return df['email'].notna().all()

def check_positive_amounts(df: pd.DataFrame) -> bool:
    return (df['amount'] > 0).all()
