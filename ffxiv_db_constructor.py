"""
Module to perform initial database creation/population for FFXIV-Market-Calculator
"""
import os
import pathlib
import sys

import requests

from sql_helpers import SqlManager


def filter_marketable_items(items, marketable_ids):
    """
    Filters items to only marketable items
    Parameters:
        items : list
            Raw item data from api
        marketable_ids : list
            Marketable item IDs
    """
    marketable_items = [None, (
        'item_num', 'name', 'ave_cost', 'regular_sale_velocity', 'ave_nq_cost', 'nq_sale_velocity',
        'ave_hq_cost', 'hq_sale_velocity', 'gatherable'), (
        'INTEGER PRIMARY KEY', 'TEXT', 'INTEGER', 'REAL', 'INTEGER', 'REAL',
        'INTEGER', 'REAL', 'TEXT DEFAULT "False" NOT NULL')]
    line_concatenate = []
    for line in items[3:]:
        split_line = line_concatenate + line.split(',')
        if len(split_line) >= 98:
            line_concatenate = []
            if split_line[0] in marketable_ids:
                marketable_items.append((split_line[0], split_line[len(split_line) - 88],
                                         0, 0, 0, 0, 0, 0, 'False'))
        else:
            line_concatenate = split_line
    return marketable_items


class FfxivDbCreation:
    """
    Class for handling the construction of databases for the script.

    Attributes:
    -------
    database : SqlManager object
        SqlManager object for database operations

    Methods:
    -------
    market_db_create():
        Creates a new database for market data
    global_db_create():
        Creates a new database for global data
    get_data_from_url():
        Retrieves data from web api
    get_marketable_ids():
        Retrieves data for marketable items
    filter_marketable_recipes():
        Filters recipes to marketable items
    filter_datacentres():
        Filters for usable datacentres
    filter_worlds():
        Filters for usable worlds
    base_state_table():
        Creates the base templated state table
    csv_to_db():
        Handles writing all data from other methods to the databases
    """
    def __init__(self, db_name):
        """
        pulls data from the universalis API to limit items to marketable items
        pulls data from:
        https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/...
            Item.csv to get item data
            Recipe.csv to get recipe data
            WorldDCGroupType.csv to get dc data
            World.csv to get world data
        creates a new database w/ the given name if it doesn't exist
        Parameters:
            db_name : str
                The name that the database should be called
        """
        if os.path.exists(pathlib.Path(__file__).parent / db_name):
            raise ValueError("Database with that name already exists")

        self.database = SqlManager(db_name)

        if db_name == os.path.join("databases", "global_db"):
            self.global_db_create()
        else:
            self.market_db_create()

    def market_db_create(self):
        """
        Creates the blank market data database
        """
        # gets a list of integers in string format ['2','3','5','6',...]
        marketable_ids = self.get_marketable_ids()
        print('Got marketable ID list')

        # gets a list of tuples containing the item data
        # index 0-2 are the column numbers, titles, and data types; index 3+ is the data
        items = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv'
        )
        print('Got raw item CSV')

        # gets a list of tuples containing the item data
        # index 0-2 are the column numbers, titles, and data types; index 3+ is the data
        recipes = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Recipe.csv')
        recipes[1] = recipes[1].replace('#', 'CSVkey')
        print('Got raw recipe CSV')

        marketable_items = filter_marketable_items(items, marketable_ids)
        print('item CSV filtered to only marketable')

        marketable_recipes = self.filter_marketable_recipes(recipes, marketable_ids)
        print('recipes CSV filtered to only marketable')

        self.csv_to_db(marketable_items, 'item')
        print('item table created')

        self.add_gatherable()
        print("gatherable flags added to item table")

        self.csv_to_db(marketable_recipes, 'recipe')
        print('recipe table created')

        for i in range(10):
            self.database.execute_query(
                f"ALTER TABLE recipe ADD ingredient_cost_{i} INTEGER DEFAULT 9999999;"
            )

        self.database.execute_query(
            "ALTER TABLE recipe ADD cost_to_craft GENERATED ALWAYS AS ("
            "amount_ingredient_0 * ingredient_cost_0 + amount_ingredient_1 * ingredient_cost_1 + "
            "amount_ingredient_2 * ingredient_cost_2 + amount_ingredient_3 * ingredient_cost_3 + "
            "amount_ingredient_4 * ingredient_cost_4 + amount_ingredient_5 * ingredient_cost_5 + "
            "amount_ingredient_6 * ingredient_cost_6 + amount_ingredient_7 * ingredient_cost_7 + "
            "amount_ingredient_8 * ingredient_cost_8 + amount_ingredient_9 * ingredient_cost_9)")

        self.database.execute_query("ALTER TABLE item ADD cost_to_craft INTEGER DEFAULT 0;")
        self.database.execute_query(
            "ALTER TABLE item ADD craft_profit GENERATED ALWAYS AS "
            "(CASE cost_to_craft WHEN 0 THEN 0 ELSE ave_cost - cost_to_craft END);"
        )
        self.database.execute_query(
            "ALTER TABLE item ADD craft_profit_per_day GENERATED ALWAYS AS "
            "(craft_profit * regular_sale_velocity);"
        )
        self.database.execute_query(
            "ALTER TABLE item ADD raw_profit_per_day GENERATED ALWAYS AS "
            "(ave_cost * regular_sale_velocity);"
        )

    def global_db_create(self):
        """
        Creates the blank global data database
        """
        # gets a list of tuples containing the datacentre data
        datacentres = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/'
            'ffxiv-datamining/master/csv/WorldDCGroupType.csv'
        )
        datacentres[1] = datacentres[1].replace('#', 'dc_key')
        print('Got raw datacentre CSV')

        # gets a list of tuples containing the world data
        worlds = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/World.csv')
        worlds[1] = worlds[1].replace('#', 'world_key')
        print('got raw world CSV')

        usable_datacentres = self.filter_datacentres(datacentres)
        print('datacentres CSV filtered to only usable')

        usable_worlds = self.filter_worlds(worlds)
        print('worlds CSV filtered to only usable')

        state_table = self.base_state_table()
        print('base state table data')

        self.csv_to_db(usable_datacentres, 'datacentre')
        print('datacentre table created')

        self.csv_to_db(usable_worlds, 'world')
        print('world table created')

        self.csv_to_db(state_table, 'state')

    @staticmethod
    def get_data_from_url(url):
        """
        pulls data from web apis
        Parameters:
            url : str
                url to perform the http request on
        """
        request_data = requests.get(url)
        if request_data.status_code == 200:
            data = request_data.content.decode('utf-8-sig')
            return data.splitlines()
        sys.exit(f"{url} not available")

    def get_marketable_ids(self):
        """
        Retrieves data for marketable items
        """
        marketable_id_data = self.get_data_from_url("https://universalis.app/api/marketable")
        marketable_ids = marketable_id_data[0].split(',')
        marketable_ids[0] = marketable_ids[0].replace('[', '')
        marketable_ids[-1] = marketable_ids[-1].replace(']', '')
        return marketable_ids

    @staticmethod
    def filter_marketable_recipes(recipes, marketable_ids):
        """
        Filters recipes to marketable items
        Parameters:
            recipes : list
                Raw recipe data from api
            marketable_ids : list
                List of marketable item IDs
        """
        marketable_recipes = recipes[0:3]
        marketable_recipes[
            1] = 'csv_key,number,craft_type,recipe_level_table,item_result,amount_result,' \
                 'item_ingredient_0,amount_ingredient_0,item_ingredient_1,amount_ingredient_1,' \
                 'item_ingredient_2,amount_ingredient_2,item_ingredient_3,amount_ingredient_3,' \
                 'item_ingredient_4,amount_ingredient_4,item_ingredient_5,amount_ingredient_5,' \
                 'item_ingredient_6,amount_ingredient_6,item_ingredient_7,amount_ingredient_7,' \
                 'item_ingredient_8,amount_ingredient_8,item_ingredient_9,amount_ingredient_9'

        marketable_recipes[
            2] = 'INTEGER PRIMARY KEY,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER'

        marketable_recipes[1] = tuple(marketable_recipes[1].split(','))
        marketable_recipes[2] = tuple(marketable_recipes[2].split(','))

        for line in recipes[3:]:
            split_line = line.split(',')
            crafted_item_id = split_line[4]
            if crafted_item_id in marketable_ids:
                marketable_recipes.append(tuple(split_line[:26]))
        return marketable_recipes

    @staticmethod
    def filter_datacentres(datacentres):
        """
        Filters for usable datacentres
        Parameters:
            datacentres : list
                Raw datacentre data from api
        """
        usable_datacentres = datacentres[0:3]
        usable_datacentres[1] = 'dc_key,name,region'
        usable_datacentres[2] = 'INTEGER PRIMARY KEY, STRING, INTEGER'
        usable_datacentres[1] = tuple(usable_datacentres[1].split(','))
        usable_datacentres[2] = tuple(usable_datacentres[2].split(','))

        for line in datacentres[3:]:
            split_line = line.split(',')
            datacentre_id = int(split_line[0])
            unquoted_line = []
            for item in split_line:
                unquoted_line.append(item.replace('"', ''))
            if 1 <= datacentre_id < 99:
                usable_datacentres.append(tuple(unquoted_line))
        return usable_datacentres

    @staticmethod
    def filter_worlds(worlds):
        """
        Filters for usable worlds
        Parameters:
            worlds : list
                Raw world data from api
        """
        usable_worlds = worlds[0:3]
        usable_worlds[1] = 'world_key, name, datacenter'
        usable_worlds[2] = 'INTEGER PRIMARY KEY, STRING, INTEGER'
        usable_worlds[1] = tuple(usable_worlds[1].split(','))
        usable_worlds[2] = tuple(usable_worlds[2].split(','))

        for line in worlds[3:]:
            split_line = line.split(',')
            world_id = int(split_line[0])
            is_public = split_line[-1]
            if is_public == 'True' and world_id != 38:
                split_line = [split_line[0], split_line[2].replace('"', ''), split_line[5]]
                usable_worlds.append(tuple(split_line))
        return usable_worlds

    @staticmethod
    def base_state_table():
        """
        Creates the base templated state table
        """
        state = ['key,0,1',
                 'marketboard_type,location,last_id',
                 'STRING,STRING NOT NULL UNIQUE,INTEGER',
                 'World,Zurvan,0']
        state[0] = tuple(state[0].split(","))
        state[1] = tuple(state[1].split(','))
        state[2] = tuple(state[2].split(','))
        state[3] = tuple(state[3].split(','))
        return state

    def add_gatherable(self):
        """
        Marks gatherable items in item table
        """
        gatherable_id_data = self.get_data_from_url("https://raw.githubusercontent.com"
                                                    "/xivapi/ffxiv-datamining/master/csv/"
                                                    "GatheringItem.csv")
        gatherable_items = []
        for item in gatherable_id_data[3:]:
            item_data = item.split(",")
            if item_data[3] == "True":
                gatherable_items.append(tuple((item_data[1],)))
        self.database.execute_query_many(
            "UPDATE item SET gatherable = 'True' WHERE item_num = ?", gatherable_items)

    # function used to convert original files into the DB
    def csv_to_db(self, csv_data, table_name):
        """
        Filters for usable worlds
        Parameters:
            csv_data : list
                Schema/data for db creation/population
            table_name : string
                Name of the table to be created in the database
        """
        database = self.database

        column_names = csv_data[1]
        data_types = csv_data[2]
        num_columns = len(column_names)

        question_marks = ""
        for _ in column_names:
            question_marks = question_marks + '?,'
        question_marks = question_marks[:-1]

        for column, name in enumerate(column_names):
            if name == "":
                column_names[column] = f'unnamedColumn{column}'

        # create a new table
        creation_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for i in range(num_columns):
            creation_command = creation_command + f"{column_names[i]} {data_types[i]}, "
        if table_name == "recipe":
            creation_command = creation_command + \
                               "FOREIGN KEY (item_result) REFERENCES item (item_num))"
        else:
            creation_command = creation_command[:-2] + ")"
        database.execute_query(creation_command)

        # insert all values
        insert_command = f"INSERT INTO {table_name} VALUES ({question_marks})"
        database.execute_query_many(insert_command, csv_data[3:])
