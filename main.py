import json
import time
import requests
import threading
import pandas as pd
from SQLhelpers import SQL_manager
from FFXIV_DB_constructor import FFXIV_DB_creation as DB_create


def API_delay():
    time.sleep(0.15)  # API only allows 7 checks/sec.


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
        if sale["timestamp"] > cutoff:  # only look at data that is < 4 weeks old
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
        ave_NQ_cost = int(total_NQ_cost / total_NQ_sales)

    if total_HQ_sales == 0:
        ave_HQ_cost = None
    else:
        ave_HQ_cost = int(total_HQ_cost / total_HQ_sales)

    if total_NQ_sales + total_HQ_sales == 0:
        ave_cost = None
    else:
        ave_cost = int((total_NQ_cost + total_HQ_cost) / (total_NQ_sales + total_HQ_sales))

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
def update_from_api(DB, start=0, location="Crystal"):
    table_name = "item"
    query = f"SELECT itemNum FROM item WHERE itemNum >= {start}"
    data = DB.return_query(query)

    for itemNum in data:
        API_delay_thread = threading.Thread(target=API_delay)
        API_delay_thread.start()
        dictionary, success = get_sale_nums(*itemNum, location)
        if success == 1:
            query = "UPDATE `{}` SET {} WHERE itemNum = %s" % itemNum
            new_data_value = ', '.join(
                ['`{}`="{}"'.format(column_name, value) for column_name, value in dictionary.items()])
            q = query.format(table_name, new_data_value)
            DB.execute_query(q)
            print(f"itemNumber {*itemNum,} updated")
        API_delay_thread.join()

    print("All Sale Data Added to Database")


# updates the ingredientCost values 0-9 for the recipe table
def update_ingredient_costs(DB):
    for i in range(10):
        numbers = DB.return_query(f"SELECT Number, ItemIngredient{i} FROM recipe")
        for num in numbers:
            ingredient_i_id = num[1]
            if not ingredient_i_id == 0:
                ave_cost = DB.return_query(f"SELECT ave_cost FROM item WHERE itemNum = {ingredient_i_id}")
                try:
                    ave_cost = ave_cost[0][0]
                except IndexError:
                    ave_cost = 9999999
                if ave_cost == "None":
                    ave_cost = 9999999
            else:
                ave_cost = 0
            DB.execute_query(f"UPDATE recipe SET ingredientCost{i} = {ave_cost} WHERE Number = {num[0]}")
        print(f"ingredientCost{i} complete")


# copies over the cost to craft data from recipes
def update_cost_to_craft(DB):
    numbers = DB.return_query("SELECT * FROM item")
    for index, num in enumerate(numbers):
        cost = DB.return_query(f"SELECT costToCraft FROM recipe WHERE ItemResult = {num[0]}")
        try:
            cost = cost[0][0]
            DB.execute_query(f"UPDATE item SET costToCraft = {cost} WHERE itemNum = {num[0]}")
        except IndexError:
            DB.execute_query(f"UPDATE item SET costToCraft = ave_cost WHERE itemNum = {num[0]}")
        if index % 100 == 0:
            print(f"{index}/{len(numbers)} updated")


# pulls data from the API, and performs all required calculations on it.
def full_update(DB, location="Crystal", start=0):
    update_from_api(DB, start=start, location=location)
    update_ingredient_costs(DB)
    update_cost_to_craft(DB)


def profit_table(DB, DB_name, velocity=10, recipeLvl=1000):
    print("\n\n")
    toDisplay = "name, craftProfit, regSaleVelocity, ave_cost, costToCraft"
    levelLimitedRecipes = f"SELECT ItemResult FROM recipe WHERE RecipeLevelTable <={recipeLvl}"
    x = DB.return_query(
        f"SELECT {toDisplay} FROM item WHERE regSaleVelocity >= {velocity} AND itemNum IN ({levelLimitedRecipes}) ORDER BY craftProfit DESC LIMIT 50")

    frame = pd.DataFrame(x)
    print(f"             Data from {DB_name} showing items w/ {velocity} or more daily sales")
    print(f"        _____________________________________________________________________________")
    frame.style.set_caption("Hello World")
    frame.columns = [toDisplay.split(", ")]
    print(frame.to_string(index=False))


def main():
    DB_name = 'market_DB_Zaler'
    try:
        DB_create(DB_name)
        print("New database created")
    except ValueError:
        print("Database already exists")
        pass

    DB = SQL_manager(DB_name)
    full_update(DB, location="Zalera", start=0)
    profit_table(DB, DB_name, velocity=20)
    # profit_table(DB, DB_name, velocity=3, recipeLvl=380)


# def calculated_column(conn):
#     # for i in range(10):
#     #     DB.execute_query(f"UPDATE recipe SET ingredientCost{i} = 0")
#     DB.execute_query("ALTER TABLE recipe DROP COLUMN costToCraft")
#     ingred_calc = "AmountIngredient0*ingredientCost0 + AmountIngredient1*ingredientCost1 + AmountIngredient2*ingredientCost2 + AmountIngredient3*ingredientCost3 + AmountIngredient4*ingredientCost4 + AmountIngredient5*ingredientCost5 + AmountIngredient6*ingredientCost6 + AmountIngredient7*ingredientCost7 + AmountIngredient8*ingredientCost8 + AmountIngredient9*ingredientCost9"
#     DB.execute_query(f"ALTER TABLE recipe ADD costToCraft AS ({ingred_calc})")


if __name__ == '__main__':
    main()
