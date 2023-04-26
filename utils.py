from sqlite3 import connect
import pandas as pd


def upload_to_db(db_path, df: pd.DataFrame, name):
    try:
        with connect(db_path) as con:
            df.to_sql(name, con, if_exists='replace', index=False)
            return True
    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return False
