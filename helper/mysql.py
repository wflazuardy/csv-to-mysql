import csv
import sys
import mysql.connector as msql
from mysql.connector import Error
from mysql.connector import MySQLConnection


class MySQL:
    def __init__(self, host: str, port: int, username: str, password: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def __enter__(self) -> None:
        self.conn = self.create_connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.conn.close()

    @staticmethod
    def create_connection(
        host: str, port: int, username: str, password: str
    ) -> MySQLConnection:
        """Create MySQLConnection object

        Args:
            host (str): MySQL host address.
            port (int): MySQL port.
            username (str): MySQL username.
            password (str): MySQL password.

        Returns:
            MySQLConnection: MySQLConnection object
        """
        try:
            conn = msql.connect(host=host, port=port, user=username, password=password)
            if conn.is_connected():
                print("MySQL connection is connected")
        except Error as e:
            print("Error while connecting to MySQL", e)
            sys.exit(e)

        return conn

    def insert_data(
        self, csv_file: str, table_name: str, schema_name: str, skip_header: bool = True
    ) -> None:
        """A method to insert data from csv file into a MySQL table.

        Args:
            csv_file (str): CSV file name.
            table_name (str): MySQL Table Name.
            schema_name (str): MySQL schema Name.
            skip_header (bool, optional): Set wether want to skip header. Defaults to True.
        """
        # Create MySQL cursor
        cursor = self.conn.cursor()

        # Write truncate -> empty table first
        cursor.execute("DELETE FROM {}.{}".format(schema_name, table_name))
        self.conn.commit()

        # Read csv file
        csv_file = open(csv_file, "r")
        data = csv.reader(csv_file)
        len_column = len(next(data))

        # Generate insert query
        insert_query = "INSERT INTO {}.{} VALUES (".format(schema_name, table_name)
        while len_column > 0:
            insert_query += "%s, "
            len_column -= 1
        insert_query = insert_query[:-2] + ")"

        # Insert data from csv file
        print("Start inserting data into table {}...".format(table_name))
        for row in data:
            if skip_header:
                skip_header = False
                continue
            # Handle empty data
            modified_row = [None if column == "" else column for column in row]
            cursor.execute(insert_query, modified_row)
        self.conn.commit()

        csv_file.close()
        print("Insert process on table {} completed.".format(table_name))
