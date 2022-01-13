from SQLhelpers import SQL_manager
import requests
import sys
import csv
import urllib


class FFXIV_DB_creation():

    def __init__(self, db_name):
        '''
        pulls data from the universalis API to limit items to marketable items
        pulls data from "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv" to get item data
        pulls data from "https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Recipe.csv" to get recipe data


        creates a new database w/ the given name

        :param db_name: str The name that the database should be called
        '''

        self.db = SQL_manager(db_name)

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

        marketable_items = self.filter_marketable_items(items, marketable_ids)
        print('item CSV filtered to only marketable')

        marketable_recipes = self.filter_marketable_recipes(recipes, marketable_ids)
        print('recipes CSV filtered to only marketable')

        self.csv_to_DB(marketable_items, 'item')
        print('item table created')

        self.csv_to_DB(marketable_recipes, 'recipe')
        print('recipe table created')

        # TODO add the additional columns like: ave_cost,regSaleVelocity,ave_NQ_cost,nqSaleVelocity,ave_HQ_cost,hqSaleVelocity'

    def get_data_from_url(self, url):
        '''
        Calls a web API to get data

        :return: content [list]
        '''
        r = requests.get(url)
        if r.status_code == 200:
            data = r.content.decode('utf-8-sig')
            return data.splitlines()
        else:
            sys.exit(f"{url} not available")

    def get_marketable_ids(self):
        marketable_idData = self.get_data_from_url("https://universalis.app/api/marketable")
        marketable_ids = marketable_idData[0].split(',')
        marketable_ids[0] = marketable_ids[0].replace('[', '')
        marketable_ids[-1] = marketable_ids[-1].replace(']', '')
        return marketable_ids

    def filter_marketable_items(self, items, marketable_ids):
        marketable_items = [None, ('itemNum', 'name'), ('INTEGER', 'TEXT')]
        line_concatenate = []
        for line in items[3:]:
            splitLine = line_concatenate + line.split(',')
            if len(splitLine) >= 98:
                line_concatenate = []
                if splitLine[0] in marketable_ids:
                    marketable_items.append((splitLine[0], splitLine[len(splitLine) - 88]))
            else:
                line_concatenate = splitLine

        # else:
        #     marketable_items.append((splitLine[0], 'NULL'))
        return marketable_items

    def filter_marketable_recipes(self, recipes, marketable_ids):
        marketable_recipes = recipes[0:3]
        marketable_recipes[
            1] = 'CSVKey,Number,CraftType,RecipeLevelTable,ItemResult,AmountResult,ItemIngredient0,AmountIngredient0,ItemIngredient1,AmountIngredient1,ItemIngredient2,AmountIngredient2,ItemIngredient3,AmountIngredient3,ItemIngredient4,AmountIngredient4,ItemIngredient5,AmountIngredient5,ItemIngredient6,AmountIngredient6,ItemIngredient7,AmountIngredient7,ItemIngredient8,AmountIngredient8,ItemIngredient9,AmountIngredient9,emptyColumn1,IsSecondary,MaterialQualityFactor,DifficultyFactor,QualityFactor,DurabilityFactor,emptyColumn2,RequiredCraftsmanship,RequiredControl,QuickSynthCraftsmanship,QuickSynthControl,SecretRecipeBook,Quest,CanQuickSynth,CanHq,ExpRewarded,StatusRequired,ItemRequired,IsSpecializationRequired,IsExpert,PatchNumber'
        marketable_recipes[
            2] = 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER'
        marketable_recipes[1] = tuple(marketable_recipes[1].split(','))
        marketable_recipes[2] = tuple(marketable_recipes[2].split(','))

        for line in recipes[3:]:
            split_line = line.split(',')
            crafted_item_id = split_line[4]
            if crafted_item_id in marketable_ids:
                marketable_recipes.append(tuple(split_line))
        return marketable_recipes

    # function used to convert original files into the DB
    def csv_to_DB(self, csv_data, table_name):
        DB = self.db

        columnNames = csv_data[1]
        dataTypes = csv_data[2]
        num_columns = len(columnNames)

        question_marks = ""
        for _ in columnNames:
            question_marks = question_marks + '?,'
        question_marks = question_marks[:-1]

        for column, name in enumerate(columnNames):
            if name == "":
                columnNames[column] = f'unnamedColumn{column}'

        # create a new table
        creation_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for i in range(num_columns):
            creation_command = creation_command + f"{columnNames[i]} {dataTypes[i]}, "
        creation_command = creation_command[:-2] + ")"  # remove the final comma and close the command
        DB.execute_query(creation_command)

        # insert all values
        insert_command = f"INSERT INTO {table_name} VALUES ({question_marks})"
        DB.execute_query_many(insert_command, csv_data[3:])
        return
