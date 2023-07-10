from sqlalchemy import create_engine, text
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Assuming you have already created an SQLAlchemy engine
engine = create_engine("postgresql://admin:admin@192.168.14.91:5431/eta")

# Construct the SQL statement
query_01 = text("""
    SELECT partman.run_maintenance('public.link_time');
""")

query_02 = text("""
    CALL partman.partition_data_proc('public.link_time', p_batch:=3)
""")

query_03 = """
    VACUUM ANALYZE public.link_time;                
"""

# Execute the query
try:
    print('Creating Future Partition')
    with engine.connect() as connection:
        connection.execute(query_01)
except:
    print('Paritioning from default')
    print('')
    print('Running CALL')
    with engine.connect() as connection:
        connection.execute(query_02)

    print('Running VACUUM ANALYZE')
    connection = engine.raw_connection()
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(query_03)
    connection.close()

    print('Creating Future Partition')
    with engine.connect() as connection:
        connection.execute(query_01)