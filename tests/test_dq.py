import pandas as pd
from src.dq.simple_checks import check_no_null_emails, check_positive_amounts

def test_check_no_null_emails():
    df = pd.DataFrame({'email': ['a@x', 'b@y']})
    assert check_no_null_emails(df)

def test_check_positive_amounts():
    df = pd.DataFrame({'amount': [1.0, 2.3, 0.01]})
    assert check_positive_amounts(df)
