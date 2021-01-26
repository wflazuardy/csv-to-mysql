from glob import glob

import config
from helper.mysql import MySQL

if __name__ == "__main__":
    MYSQL_SCHEMA = config.MYSQL_SCHEMA
    # Get csv files
    csv_files = glob("data/*.csv")
    for csv_file in csv_files:
        mysql_client = MySQL(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            username=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
        )

        table_name = csv_file[5:-4]
        # Insert data to MySQL
        with mysql_client as mysql:
            mysql.insert_data(
                csv_file=csv_file, table_name=table_name, schema_name=MYSQL_SCHEMA
            )

        del mysql_client
