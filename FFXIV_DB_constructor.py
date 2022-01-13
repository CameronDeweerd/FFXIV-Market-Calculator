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

        # gets a list of strings containing the item data in string format ['data,data,data,data', 'data,data,data,data',...]
        # index 0-2 are the column numbers, titles, and datatypes; index 3+ is the data
        items = self.get_data_from_url('https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Item.csv')
        print('Got item CSV')

        # gets a list of strings containing the item data in string format ['data,data,data,data', 'data,data,data,data',...]
        # index 0-2 are the column numbers, titles, and datatypes; index 3+ is the data
        recipes = self.get_data_from_url(
            'https://raw.githubusercontent.com/xivapi/ffxiv-datamining/master/csv/Recipe.csv')
        recipes[1] = recipes[1].replace('#', 'CSVkey')
        print('Got recipe CSV')

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
            return r.content.decode('utf-8-sig').splitlines()
        else:
            sys.exit(f"{url} not available")

    def get_marketable_ids(self):
        marketable_idData = self.get_data_from_url("https://universalis.app/api/marketable")
        marketable_ids = marketable_idData[0].split(',')
        marketable_ids[0] = marketable_ids[0].replace('[', '')
        marketable_ids[-1] = marketable_ids[-1].replace(']', '')
        return marketable_ids

    def filter_marketable_items(self, items, marketable_ids):
        '''
        :param items: list The name that the database should be called
        :param marketable_ids: list The name that the database should be called

        :return: List of item IDs that can be sold on the market
        '''

        marketable_items = [None, 'itemNum,name', 'INTEGER,TEXT']
        for line in items[3:]:
            splitLine = line.split(',')
            if splitLine[0] in marketable_ids:
                marketable_items.append(f'{splitLine[0]},NULL')
        return marketable_items

    def filter_marketable_recipes(self, recipes, marketable_ids):
        marketable_recipes = recipes[0:3]
        marketable_recipes[
            1] = 'CSVKey,Number,CraftType,RecipeLevelTable,ItemResult,AmountResult,ItemIngredient0,AmountIngredient0,ItemIngredient1,AmountIngredient1,ItemIngredient2,AmountIngredient2,ItemIngredient3,AmountIngredient3,ItemIngredient4,AmountIngredient4,ItemIngredient5,AmountIngredient5,ItemIngredient6,AmountIngredient6,ItemIngredient7,AmountIngredient7,ItemIngredient8,AmountIngredient8,ItemIngredient9,AmountIngredient9,emptyColumn1,IsSecondary,MaterialQualityFactor,DifficultyFactor,QualityFactor,DurabilityFactor,emptyColumn2,RequiredCraftsmanship,RequiredControl,QuickSynthCraftsmanship,QuickSynthControl,SecretRecipeBook,Quest,CanQuickSynth,CanHq,ExpRewarded,StatusRequired,ItemRequired,IsSpecializationRequired,IsExpert,PatchNumber'
        marketable_recipes[
            2] = 'INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER,INTEGER'
        for line in recipes[3:]:
            crafted_item_id = line.split(',')[4]
            if crafted_item_id in marketable_ids:
                marketable_recipes.append(line)
        return marketable_recipes

    # function used to convert original files into the DB
    def csv_to_DB(self, csv_data, table_name):
        DB = self.db

        dataTypes = csv_data[2].split(',')
        columnNames = csv_data[1].split(',')
        for column, name in enumerate(columnNames):
            if name == "":
                columnNames[column] = f'unnamedColumn{column}'

        # create a new table
        creation_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for i in range(len(columnNames)):
            creation_command = creation_command + f"{columnNames[i]} {dataTypes[i]}, "
        creation_command = creation_command[:-2] + ")"  # remove the final comma and close the command
        DB.execute_query(creation_command)

        # add all values
        for index, dataRow in enumerate(csv_data[3:]):
            insert_command = f"INSERT INTO {table_name} {*columnNames,} VALUES ({dataRow})"
            DB.execute_query(insert_command)
            if index % 100 == 0:
                print(f'{index} out of {len(csv_data) - 3} added')
        return

# # function used to convert original files into the DB
# def csv_to_DB(self, csv_filename):
#     DB = self.db
#     target = csv_filename + ".csv"
#
#     with open(target, newline='', encoding='utf-8-sig') as f:
#         reader = csv.reader(f)
#         columnNames = next(reader)  # gets the column names)
#         dataTypes = next(f).split(",")  # gets the datatypes line)
#
#         # create a new table
#         creation_command = f"CREATE TABLE IF NOT EXISTS {csv_filename} ("
#         for i in range(len(columnNames)):
#             creation_command = creation_command + f"{columnNames[i]} {dataTypes[i]}, "
#         creation_command = creation_command[:-2] + ")"  # remove the final comma and close the command
#         DB.execute_query(creation_command)
#
#         index = 0
#         # add all values
#         for dataRow in f:
#             index += 1
#             insert_command = f"INSERT INTO {csv_filename} {*columnNames,} VALUES ({dataRow})"
#             DB.execute_query(insert_command)
#             print(index)
#         f.close()
#         return
