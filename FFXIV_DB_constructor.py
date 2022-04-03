import os
import pathlib
import sys

import requests

from SQLhelpers import SqlManager


def filter_marketable_items(items, marketable_ids):
    marketable_items = [None, (
        'item_num', 'name', 'ave_cost', 'regular_sale_velocity', 'ave_nq_cost', 'nq_sale_velocity',
        'ave_hq_cost', 'hq_sale_velocity'), (
        'INTEGER PRIMARY KEY', 'TEXT', 'INTEGER', 'REAL', 'INTEGER', 'REAL',
        'INTEGER', 'REAL')]
    line_concatenate = []
    for line in items[3:]:
        split_line = line_concatenate + line.split(',')
        if len(split_line) >= 98:
            line_concatenate = []
            if split_line[0] in marketable_ids:
                marketable_items.append((split_line[0], split_line[len(split_line) - 88], 'NULL', 'NULL', 'NULL',
                                         'NULL', 'NULL', 'NULL'))
        else:
            line_concatenate = split_line
    return marketable_items


class FfxivDbCreation:

    def __init__(self, db_name):
        """
        pulls data from the universalis API to limit items to marketable items
        pulls data from:
         "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv" to get item data
         "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Recipe.csv" to get recipe data
         "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/WorldDCGroupType.csv" to get dc data
         "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/World.csv" to get world data


        creates a new database w/ the given name

        :param db_name: str The name that the database should be called
        """
        if os.path.exists(pathlib.Path(__file__).parent / db_name):
            raise ValueError("Database with that name already exists")

        self.db = SqlManager(db_name)

        if db_name == os.path.join("databases", "global_db"):
            self.global_db_create()
        else:
            self.market_db_create()

    def market_db_create(self):
        # gets a list of integers in string format ['2','3','5','6',...]
        marketable_ids = self.get_marketable_ids()
        print('Got marketable ID list')

        # gets a list of tuples containing the item data
        # index 0-2 are the column numbers, titles, and data types; index 3+ is the data
        items = self.get_data_from_url('https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv')
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

        self.csv_to_db(marketable_recipes, 'recipe')
        print('recipe table created')

        for i in range(10):
            self.db.execute_query(f"ALTER TABLE recipe ADD ingredient_cost_{i} INTEGER DEFAULT 0;")

        self.db.execute_query(
            "ALTER TABLE recipe ADD cost_to_craft GENERATED ALWAYS AS ("
            "amount_ingredient_0 * ingredient_cost_0 + amount_ingredient_1 * ingredient_cost_1 + "
            "amount_ingredient_2 * ingredient_cost_2 + amount_ingredient_3 * ingredient_cost_3 + "
            "amount_ingredient_4 * ingredient_cost_4 + amount_ingredient_5 * ingredient_cost_5 + "
            "amount_ingredient_6 * ingredient_cost_6 + amount_ingredient_7 * ingredient_cost_7 + "
            "amount_ingredient_8 * ingredient_cost_8 + amount_ingredient_9 * ingredient_cost_9)")

        self.db.execute_query(f"ALTER TABLE item ADD cost_to_craft INTEGER DEFAULT 0;")
        # self.db.execute_query(
        #     f"ALTER TABLE item ADD costToCraft GENERATED ALWAYS AS (SELECT costToCraft
        #     FROM recipe WHERE ItemResult = itemNum LIMIT 1);")
        self.db.execute_query(f"ALTER TABLE item ADD craft_profit GENERATED ALWAYS AS (ave_cost - cost_to_craft);")

        # TODO add the additional columns like:
        #  ave_cost,regSaleVelocity,ave_NQ_cost,nqSaleVelocity,ave_HQ_cost,hqSaleVelocity'

    def global_db_create(self):
        # gets a list of tuples containing the datacentre data
        datacentres = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/WorldDCGroupType.csv')
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
        Calls a web API to get data

        :return: content [list]
        """
        r = requests.get(url)
        if r.status_code == 200:
            data = r.content.decode('utf-8-sig')
            return data.splitlines()
        else:
            sys.exit(f"{url} not available")

    def get_marketable_ids(self):
        marketable_id_data = self.get_data_from_url("https://universalis.app/api/marketable")
        marketable_ids = marketable_id_data[0].split(',')
        marketable_ids[0] = marketable_ids[0].replace('[', '')
        marketable_ids[-1] = marketable_ids[-1].replace(']', '')
        return marketable_ids

    @staticmethod
    def filter_marketable_recipes(recipes, marketable_ids):
        marketable_recipes = recipes[0:3]
        marketable_recipes[
            1] = 'csv_key,number,craft_type,recipe_level_table,item_result,amount_result,' \
                 'item_ingredient_0,amount_ingredient_0,item_ingredient_1,amount_ingredient_1,' \
                 'item_ingredient_2,amount_ingredient_2,item_ingredient_3,amount_ingredient_3,' \
                 'item_ingredient_4,amount_ingredient_4,item_ingredient_5,amount_ingredient_5,' \
                 'item_ingredient_6,amount_ingredient_6,item_ingredient_7,amount_ingredient_7,' \
                 'item_ingredient_8,amount_ingredient_8,item_ingredient_9,amount_ingredient_9,' \
                 'empty_column_1,is_secondary,material_quality_factor,difficulty_factor,quality_factor,' \
                 'durability_factor,empty_column_2,required_craftsmanship,required_control,quick_synth_craftsmanship,' \
                 'quick_synth_control,secret_recipe_book,quest,can_quick_synth,can_hq,exp_rewarded,' \
                 'status_required,item_required,is_specialization_required,is_expert,patch_number'
        marketable_recipes[
            2] = 'INTEGER PRIMARY KEY,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,' \
                 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER'
        marketable_recipes[1] = tuple(marketable_recipes[1].split(','))
        marketable_recipes[2] = tuple(marketable_recipes[2].split(','))

        for line in recipes[3:]:
            split_line = line.split(',')
            crafted_item_id = split_line[4]
            if crafted_item_id in marketable_ids:
                marketable_recipes.append(tuple(split_line))
        return marketable_recipes

    @staticmethod
    def filter_datacentres(datacentres):
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
        state = ['key,0,1',
                 'marketboard_type,location,last_id',
                 'STRING,STRING NOT NULL UNIQUE,INTEGER',
                 'World,Zurvan,0']
        state[0] = tuple(state[0].split(","))
        state[1] = tuple(state[1].split(','))
        state[2] = tuple(state[2].split(','))
        state[3] = tuple(state[3].split(','))
        print(state)
        return state

    # function used to convert original files into the DB
    def csv_to_db(self, csv_data, table_name):
        db = self.db

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
        creation_command = creation_command[:-2] + ")"  # remove the final comma and close the command
        db.execute_query(creation_command)

        # insert all values
        insert_command = f"INSERT INTO {table_name} VALUES ({question_marks})"
        db.execute_query_many(insert_command, csv_data[3:])
        return
