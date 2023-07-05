import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
query = """
select *
from link_data;
"""
link_data = pd.read_sql(query, engine)
link_data.to_csv('/usr/local/spark/resources/data/tmp/mdvr_data/link_data.csv', index=False, encoding='utf-8-sig')