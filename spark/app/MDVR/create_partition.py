from sqlalchemy import create_engine, text

# Assuming you have already created an SQLAlchemy engine
engine = create_engine("postgresql://admin:admin@192.168.14.91:5431/eta")

# Construct the SQL statement
# query_01 = text("""
#     CALL partman.partition_data_proc('public.link_time', p_batch:=3)
# """)

query = text("""
    SELECT partman.run_maintenance('public.link_time');
""")

# query_03 = text("""
#     VACUUM ANALYZE public.link_time;                
# """)

# Execute the query
with engine.connect() as connection:
    # connection.execute(query_01)
    connection.execute(query)
    # connection.execute(query_03)
    
# Note that in most cases, you don't need to manually close the connection when using the with statement 
# because the connection is automatically closed when you exit the with block.