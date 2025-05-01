import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv("job_analysis.csv")
engine = create_engine("sqlite:///job_analysis.db")
df.to_sql("job_analysis", engine, index=False)

