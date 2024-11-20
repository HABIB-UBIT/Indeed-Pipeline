from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

username = 'root'
password = '03102000'
db_name = 'jobsapidata'
host = 'localhost'
port = 3306

try:
    # Use MySQL Connector with timeout settings
    connection_string = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{db_name}?charset=utf8mb4"
    engine = create_engine(connection_string, pool_recycle=3600, pool_size=10, max_overflow=20)

    # Test the connection
    with engine.connect() as connection:
        print("Connected to MySQL Server version", connection.connection.get_server_info())

except SQLAlchemyError as e:
    print("Error:", e)
