import sqlite3
import csv
import pandas as pd
import requests
import json
import time


# function used to convert original files into the DB
def csv_to_DB(connection, filename):
    target = filename + ".csv"
    with open(target, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        columnNames = next(reader)         # gets the column names)
        dataTypes = next(f).split(",")   # gets the datatypes line)

        # create a new table
        creation_command = f"CREATE TABLE IF NOT EXISTS {filename} ("
        for i in range(len(columnNames)):
            creation_command = creation_command + f"{columnNames[i]} {dataTypes[i]}, "
        creation_command = creation_command[:-2] + ")"  # remove the final comma and close the command
        execute_query(connection, creation_command)

        index = 0
        # add all values
        for dataRow in f:
            index += 1
            insert_command = f"INSERT INTO {filename} {*columnNames,} VALUES ({dataRow})"
            execute_query(connection, insert_command)
            print(index)

        f.close()
        connection.commit()
        return


# gets the velocity and sale data and creates a dict with it
def get_sale_nums(itemNum, location="Crystal"):
    sale_data = {
        "regSaleVelocity": None,
        "nqSaleVelocity": None,
        "hqSaleVelocity": None,
        "ave_NQ_cost": None,
        "ave_HQ_cost": None,
        "ave_cost": None
    }

    data, r = get_sale_data(itemNum, location)
    if r.status_code == 404 or not data:
        print(f"itemNumber {*itemNum,} found no data")
        return sale_data, 0

    try:
        if data["regularSaleVelocity"] > 142:
            data, r = get_sale_data(itemNum, location, 10000)
        regSaleVelocity = round(data["regularSaleVelocity"], 1)
        nqSaleVelocity = round(data["nqSaleVelocity"], 1)
        hqSaleVelocity = round(data["hqSaleVelocity"], 1)
        if regSaleVelocity == 0:
            return sale_data, 1
    except Exception as err:
        print(f"{err} w/ itemNum {itemNum}")
        return sale_data, 0

    sales = data["entries"]
    total_NQ_cost = 0
    total_NQ_sales = 0
    total_HQ_cost = 0
    total_HQ_sales = 0

    # calculate the sales for nq and hq
    cutoff = time.time() - 86400 * 28
    for sale in sales:
        if sale["timestamp"] > cutoff:      # only look at data that is < 4 weeks old
            try:
                if sale["pricePerUnit"] < 1000000:
                    if not sale["hq"]:
                        total_NQ_cost += sale["pricePerUnit"] * sale["quantity"]
                        total_NQ_sales += sale["quantity"]
                    else:
                        total_HQ_cost += sale["pricePerUnit"] * sale["quantity"]
                        total_HQ_sales += sale["quantity"]
            except:
                return sale_data, 1
        else:
            break

    # get averages and avoid divide by zero
    if total_NQ_sales == 0:
        ave_NQ_cost = None
    else:
        ave_NQ_cost = int(total_NQ_cost/total_NQ_sales)

    if total_HQ_sales == 0:
        ave_HQ_cost = None
    else:
        ave_HQ_cost = int(total_HQ_cost/total_HQ_sales)

    if total_NQ_sales + total_HQ_sales == 0:
        ave_cost = None
    else:
        ave_cost = int((total_NQ_cost + total_HQ_cost)/(total_NQ_sales + total_HQ_sales))

    # create dict of info we care about
    sale_data = {
        "regSaleVelocity": regSaleVelocity,
        "nqSaleVelocity": nqSaleVelocity,
        "hqSaleVelocity": hqSaleVelocity,
        "ave_NQ_cost": ave_NQ_cost,
        "ave_HQ_cost": ave_HQ_cost,
        "ave_cost": ave_cost
    }
    return sale_data, 1


# Calls the Universalis API and returns the data and the status code
def get_sale_data(itemNum, location="Crystal", entries=1000):
    r = requests.get(f'https://universalis.app/api/history/{location}/{itemNum}?entries={entries}')
    try:
        data = json.loads(r.content.decode('utf-8'))
        return data, r
    except:
        return None, r

# puts the data from the Universalis API into the DB
def update_from_api(conn, start=0, location="Crystal"):
    table_name = "item"
    query = f"SELECT itemNum FROM item WHERE IsUntradable IS 0 AND itemNum > {start}"
    data = return_query(conn, query)

    for itemNum in data:

        dictionary, success = get_sale_nums(*itemNum, location)
        if success == 1:
            query = "UPDATE `{}` SET {} WHERE itemNum = %s" % itemNum
            new_data_value = ', '.join(['`{}`="{}"'.format(column_name, value) for column_name, value in dictionary.items()])
            q = query.format(table_name, new_data_value)
            execute_query(conn, q)
            print(f"itemNumber {*itemNum,} updated")
        time.sleep(.15)     # API only allows 7 checks/sec.
    print("All Sale Data Added to Database")


# updates the ingredientCost values 0-9 for the recipe table
def update_ingredient_costs(conn):

    for i in range(10):
        numbers = return_query(conn, "SELECT * FROM recipe WHERE Number > 10000")
        for num in numbers:
            if num[5 + 3*i] != 0:
                ave_cost = return_query(conn, f"SELECT ave_cost FROM item WHERE itemNum = {num[5 + 3*i]}")
                try:
                    ave_cost = ave_cost.fetchone()[0]
                except TypeError:
                    ave_cost = 999999
            else:
                ave_cost = 0
            execute_query(conn, f"UPDATE recipe SET ingredientCost{i} = {ave_cost} WHERE Number = {num[0]}")
        print(f"ingredientCost{i} complete")


# copies over the cost to craft data from recipes
def update_cost_to_craft(conn):
    numbers = return_query(conn, "SELECT * FROM item")
    for num in numbers:
        cost = return_query(conn, f"SELECT costToCraft FROM recipe WHERE ItemResult = {num[0]}")
        try:
            cost = cost.fetchone()[0]
            execute_query(conn, f"UPDATE item SET costToCraft = {cost} WHERE itemNum = {num[0]}")
        except TypeError:
            execute_query(conn, f"UPDATE item SET costToCraft = ave_cost WHERE itemNum = {num[0]}")
        print(f"{num[0]} updated")


# Helper function for SQL execution when returns are unneeded
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        # print("Query successful")
    except Exception as err:
        print(f"Error: '{err}'")


# Helper function for SQL execution when returns are needed
def return_query(connection, query):
    cursor = connection.cursor()
    try:
        result = cursor.execute(query)
        return result
    except Exception as err:
        print(f"Error: '{err}'")


# pulls data from the API, and performs all required calculations on it.
def full_update(conn, location="Crystal", start=0):

    update_from_api(conn, start=start, location=location)
    update_ingredient_costs(conn)
    update_cost_to_craft(conn)


def profit_table(conn, DB, velocity=10, recipeLvl=1000):
    print("\n\n")
    toDisplay = "name, craftProfit, regSaleVelocity, ave_cost, costToCraft"
    levelLimitedRecipes = f"SELECT ItemResult FROM recipe WHERE RecipeLevelTable <={recipeLvl}"
    x = return_query(conn, f"SELECT {toDisplay} FROM item WHERE regSaleVelocity >= {velocity} AND itemNum IN ({levelLimitedRecipes}) ORDER BY craftProfit DESC LIMIT 50")

    frame = pd.DataFrame(x.fetchall())
    print(f"             Data from {DB} showing items w/ {velocity} or more daily sales")
    print(f"        _____________________________________________________________________________")
    frame.style.set_caption("Hello World")
    frame.columns = [toDisplay.split(", ")]
    print(frame.to_string(index=False))



if __name__ == '__main__':
    DB = 'market_DB_Zalera'
    connect = sqlite3.connect(DB)

    # full_update(connect, location="Zalera", start=18086)
    profit_table(connect, DB, velocity=3, recipeLvl=380)

    connect.close()




# def calculated_column(conn):
#     # for i in range(10):
#     #     execute_query(conn, f"UPDATE recipe SET ingredientCost{i} = 0")
#     execute_query(conn, "ALTER TABLE recipe DROP COLUMN costToCraft")
#     ingred_calc = "AmountIngredient0*ingredientCost0 + AmountIngredient1*ingredientCost1 + AmountIngredient2*ingredientCost2 + AmountIngredient3*ingredientCost3 + AmountIngredient4*ingredientCost4 + AmountIngredient5*ingredientCost5 + AmountIngredient6*ingredientCost6 + AmountIngredient7*ingredientCost7 + AmountIngredient8*ingredientCost8 + AmountIngredient9*ingredientCost9"
#     execute_query(conn, f"ALTER TABLE recipe ADD costToCraft AS ({ingred_calc})")
