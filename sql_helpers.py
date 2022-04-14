"""All SQL related helper functions to be kept here"""
import sqlite3


class SqlManager:
    """
    Class for handling the sql helpers.

    Attributes:
    -------
    database : str
        Database path/filename

    Methods:
    -------
    sql_connect():
        Creates a new Discord Message
    execute_query(query, options):
        Helper function for SQL execution when returns are unneeded
    execute_query_many(query, options):
        Helper function for SQL execution when returns are unneeded
    return_query(query, options):
        Helper function for SQL execution when returns are needed
    """
    def __init__(self, db_name):
        """
        Constructs all the necessary attributes for the SqlManager object.

        Parameters:
            db_name : str
                Database path/filename
        """
        self.database = db_name
        self.sql_connect().close()  # create a new DB if it doesn't exist

    def sql_connect(self):
        """
        Performs the database connection
        """
        connection = sqlite3.connect(self.database)
        return connection

    def execute_query(self, query, options=[]):
        """
        Helper function for SQL execution when returns are unneeded.

        Parameters:
            query : str
                SQLite3 query to run
            options : list
                SQLite3 options
        """
        connection = self.sql_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                cursor.execute(query)
            else:
                cursor.execute(query, options)
            connection.commit()
            # print("Query successful")
        except connection.Error as err:
            print(f"Error: '{err}'")
            print(f"Query: {query}")
        cursor.close()
        connection.close()

    def execute_query_many(self, query, options=[]):
        """
        Helper function for SQL execution of many queries when returns are unneeded.

        Parameters:
            query : list
                SQLite3 queries to run
            options : list
                SQLite3 options
        """
        connection = self.sql_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                cursor.executemany(query)
            else:
                cursor.executemany(query, options)
            connection.commit()
            # print("Query successful")
        except connection.Error as err:
            print(f"Error: '{err}'")
            print(f"Many Query: {query}")
        cursor.close()
        connection.close()

    def return_query(self, query, options=[]):
        """
        Helper function for SQL execution when returns are needed.

        Parameters:
            query : str
                SQLite3 query to run
            options : list
                SQLite3 options
        """
        connection = self.sql_connect()
        cursor = connection.cursor()
        try:
            if len(options) == 0:
                result = cursor.execute(query)
            else:
                result = cursor.execute(query, options)
            result = result.fetchall()
            return result
        except connection.Error as err:
            print(f"Error: '{err}'")
            print(f"Return Query: {query}")
        cursor.close()
        connection.close()
        return None
