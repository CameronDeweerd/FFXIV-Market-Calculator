"""All SQL related helper functions to be kept here"""
import sqlite3


class SQL_manager():
    def __init__(self, db_name):
        self.db = db_name
        self.SQL_connect().close()  # opens and closes a connection to create a new DB if it doesn't exist

    def SQL_connect(self):
        connection = sqlite3.connect(self.db)
        return connection

    # Helper function for SQL execution when returns are unneeded
    def execute_query(self, query, options=[]):
        connection = self.SQL_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                cursor.execute(query)
            else:
                cursor.execute(query, options)
            connection.commit()
            # print("Query successful")
        except Exception as err:
            print(f"Error: '{err}'")
        cursor.close()
        connection.close()

    # Helper function for SQL execution when returns are unneeded
    def execute_query_many(self, query, options=[]):
        connection = self.SQL_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                cursor.executemany(query)
            else:
                cursor.executemany(query, options)
            connection.commit()
            # print("Query successful")
        except Exception as err:
            print(f"Error: '{err}'")
        cursor.close()
        connection.close()

    # Helper function for SQL execution when returns are needed
    def return_query(self, query, options=[]):
        connection = self.SQL_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                result = cursor.execute(query)
            else:
                result = cursor.execute(query, options)
            result = result.fetchall()
            return result
        except Exception as err:
            print(f"Error: '{err}'")
        cursor.close()
        connection.close()
