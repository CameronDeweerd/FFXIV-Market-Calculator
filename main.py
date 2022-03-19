import configparser
import json
import os
import pandas as pd
import requests
import threading
import time

from FFXIV_DB_constructor import FfxivDbCreation as Db_Create
from SQLhelpers import SqlManager


def load_config():
    cfg = configparser.ConfigParser()
    cfg.read("config.ini")
    marketboard_type = cfg["MAIN"].get('MarketboardType', 'world')
    datacentre = cfg["MAIN"].get('Datacentre', 'Crystal')
    world = cfg["MAIN"].get('World', 'Zalera')
    result_quantity = cfg["MAIN"].getint('ResultQuantity', 50)
    update_quantity = cfg["MAIN"].getint('UpdateQuantity', 0)
    config_dict = {
        "marketboard_type": marketboard_type.capitalize(),
        "datacentre": datacentre.capitalize(),
        "world": world.capitalize(),
        "result_quantity": result_quantity,
        "update_quantity": update_quantity
    }
    return config_dict


def config_validation(config_dict, global_db):

    if config_dict["marketboard_type"] not in ["World", "Datacentre", "Datacenter"]:
        print("Config Error: Marketboard Type is Unknown, this value should be World, Datacentre, or Datacenter")
        exit()

    if config_dict["datacentre"] not in global_db.return_query(f'SELECT Name FROM datacentre'):
        print("Config Error: Datacentre is not in Database, check config")
        exit()

    if config_dict["world"] not in global_db.return_query(f'SELECT Name FROM world'):
        print("Config Error: World is not in Database, check config")
        exit()

    if isinstance(config_dict["result_quantity"], int) and config_dict["result_quantity"] > 0:
        print("Config Error: Result Quantity must be a whole number and greater than 0")
        exit()

    if isinstance(config_dict["update_quantity"], int):
        print("Config Error: Update Quantity must be a whole number")
        exit()


def api_delay():
    time.sleep(0.15)  # API only allows 7 checks/sec.


# gets the velocity and sale data and creates a dict with it
def get_sale_nums(item_number, location):
    sale_data = {
        "regular_sale_velocity": None,
        "nq_sale_velocity": None,
        "hq_sale_velocity": None,
        "ave_nq_cost": None,
        "ave_hq_cost": None,
        "ave_cost": None
    }

    data, r = get_sale_data(item_number, location)
    if r.status_code == 404 or not data:
        print(f"item_number {*item_number,} found no data")
        return sale_data, 0

    try:
        if data["regularSaleVelocity"] > 142:
            data, r = get_sale_data(item_number, location, 10000)
        regular_sale_velocity = round(data["regularSaleVelocity"], 1)
        nq_sale_velocity = round(data["nq_sale_velocity"], 1)
        hq_sale_velocity = round(data["hq_sale_velocity"], 1)
        if regular_sale_velocity == 0:
            return sale_data, 1
    except Exception as err:
        print(f"{err} w/ item_number {item_number}")
        return sale_data, 0

    sales = data["entries"]
    total_nq_cost = 0
    total_nq_sales = 0
    total_hq_cost = 0
    total_hq_sales = 0

    # calculate the sales for nq and hq
    cutoff = time.time() - 86400 * 28
    for sale in sales:
        if sale["timestamp"] > cutoff:  # only look at data that is < 4 weeks old
            try:
                if sale["pricePerUnit"] < 1000000:
                    if not sale["hq"]:
                        total_nq_cost += sale["pricePerUnit"] * sale["quantity"]
                        total_nq_sales += sale["quantity"]
                    else:
                        total_hq_cost += sale["pricePerUnit"] * sale["quantity"]
                        total_hq_sales += sale["quantity"]
            except:
                return sale_data, 1
        else:
            break

    # get averages and avoid divide by zero
    if total_nq_sales == 0:
        ave_nq_cost = None
    else:
        ave_nq_cost = int(total_nq_cost / total_nq_sales)

    if total_hq_sales == 0:
        ave_hq_cost = None
    else:
        ave_hq_cost = int(total_hq_cost / total_hq_sales)

    if total_nq_sales + total_hq_sales == 0:
        ave_cost = None
    else:
        ave_cost = int((total_nq_cost + total_hq_cost) / (total_nq_sales + total_hq_sales))

    # create dict of info we care about
    sale_data = {
        "regular_sale_velocity": regular_sale_velocity,
        "nq_sale_velocity": nq_sale_velocity,
        "hq_sale_velocity": hq_sale_velocity,
        "ave_nq_cost": ave_nq_cost,
        "ave_hq_cost": ave_hq_cost,
        "ave_cost": ave_cost
    }
    return sale_data, 1


# Calls the Universalis API and returns the data and the status code
def get_sale_data(item_number, location, entries=1000):
    r = requests.get(f'https://universalis.app/api/history/{location}/{item_number}?entries={entries}')
    try:
        data = json.loads(r.content.decode('utf-8'))
        return data, r
    except:
        return None, r


# puts the data from the Universalis API into the db
def update_from_api(db, location, start_id, update_quantity):
    table_name = "item"
    if update_quantity == 0:
        query = f"SELECT item_number FROM item WHERE item_number >= {start_id}"
    else:
        query = f"SELECT item_number FROM item WHERE item_number >= {start_id} " \
                f"ORDER BY item_number ASC LIMIT {update_quantity}"
    data = db.return_query(query)

    for item_number in data:
        api_delay_thread = threading.Thread(target=api_delay)
        api_delay_thread.start()
        dictionary, success = get_sale_nums(*item_number, location)
        if success == 1:
            query = "UPDATE `{}` SET {} WHERE item_number = %s" % item_number
            new_data_value = ', '.join(
                ['`{}`="{}"'.format(column_name, value) for column_name, value in dictionary.items()])
            q = query.format(table_name, new_data_value)
            db.execute_query(q)
            print(f"item_number {*item_number,} updated")
        api_delay_thread.join()

    print("All Sale Data Added to Database")


# updates the ingredientCost values 0-9 for the recipe table
def update_ingredient_costs(db):
    for i in range(10):
        numbers = db.return_query(f"SELECT Number, ItemIngredient{i} FROM recipe")
        for num in numbers:
            ingredient_i_id = num[1]
            if not ingredient_i_id == 0:
                ave_cost = db.return_query(f"SELECT ave_cost FROM item WHERE item_number = {ingredient_i_id}")
                try:
                    ave_cost = ave_cost[0][0]
                except IndexError:
                    ave_cost = 9999999
                if ave_cost == "None":
                    ave_cost = 9999999
            else:
                ave_cost = 0
            db.execute_query(f"UPDATE recipe SET ingredientCost{i} = {ave_cost} WHERE Number = {num[0]}")
        print(f"ingredientCost{i} complete")


# copies over the cost to craft data from recipes
def update_cost_to_craft(db):
    numbers = db.return_query("SELECT * FROM item")
    for index, num in enumerate(numbers):
        cost = db.return_query(f"SELECT costToCraft FROM recipe WHERE ItemResult = {num[0]}")
        try:
            cost = cost[0][0]
            db.execute_query(f"UPDATE item SET costToCraft = {cost} WHERE item_number = {num[0]}")
        except IndexError:
            db.execute_query(f"UPDATE item SET costToCraft = ave_cost WHERE item_number = {num[0]}")
        if index % 100 == 0:
            print(f"{index}/{len(numbers)} updated")


# pulls data from the API, and performs all required calculations on it.
def update(db, location, start_id, update_quantity):
    update_from_api(db, location, start_id, update_quantity)
    update_ingredient_costs(db)
    update_cost_to_craft(db)


def profit_table(db, db_name, result_quantity, velocity=10, recipe_lvl=1000):
    print("\n\n")
    to_display = "name, craftProfit, regular_sale_velocity, ave_cost, costToCraft"
    level_limited_recipes = f"SELECT ItemResult FROM recipe WHERE RecipeLevelTable <={recipe_lvl}"
    x = db.return_query(
        f"SELECT {to_display} FROM item WHERE regular_sale_velocity >= {velocity} AND item_number "
        f"IN ({level_limited_recipes}) ORDER BY craftProfit DESC LIMIT {result_quantity}")

    frame = pd.DataFrame(x)
    print(f"             Data from {db_name} showing items w/ {velocity} or more daily sales")
    print(f"        _____________________________________________________________________________")
    frame.style.set_caption("Hello World")
    frame.columns = [to_display.split(", ")]
    print(frame.to_string(index=False))


def main():
    global_db_path = os.path.join("databases", "global_db")
    try:
        Db_Create(global_db_path)
        print("New Global DB Created")
    except ValueError:
        print("Global Database already exists")
        pass
    global_db = SqlManager(global_db_path)

    config_dict = load_config()
    config_validation(config_dict, global_db)

    marketboard_type = config_dict["marketboard_type"]

    if marketboard_type == "World":
        location = config_dict["world"]
    elif marketboard_type == "Datacentre" or marketboard_type == "Datacenter":
        location = config_dict["datacentre"]
    else:
        print("Invalid Config MarketboardType Selection, Field should be one of Datacentre/Datacenter/World")
        exit()
    market_db_name = os.path.join("databases", marketboard_type + "_" + location)

    try:
        Db_Create(market_db_name)
        print("New World or DC database created")
    except ValueError:
        print("World or DC Database already exists")
        pass

    selected_location_start_id = global_db.return_query(
        f'SELECT LastId FROM state WHERE '
        f'MarketboardType = {marketboard_type} AND Location = {location}'
    )

    if len(selected_location_start_id) > 0:
        start_id = int(selected_location_start_id[0])
    else:
        start_id = 0
        global_db.execute_query(
            f'INSERT INTO state (MarketboardType, Location, LastId) '
            f'VALUES("{marketboard_type}", "{location}", 0)'
        )

    location_db = SqlManager(market_db_name)
    update_quantity = int(config_dict["update_quantity"])
    update(location_db, location, start_id, update_quantity)
    result_quantity = int(config_dict["result_quantity"])
    profit_table(location_db, market_db_name, result_quantity, velocity=20)
    # profit_table(db, db_name, velocity=3, recipe_lvl=380)


# def calculated_column(conn):
#     # for i in range(10):
#     #     db.execute_query(f"UPDATE recipe SET ingredientCost{i} = 0")
#     db.execute_query("ALTER TABLE recipe DROP COLUMN costToCraft")
#     ingred_calc = "AmountIngredient0*ingredientCost0 + AmountIngredient1*ingredientCost1 +
#     AmountIngredient2*ingredientCost2 + AmountIngredient3*ingredientCost3 + AmountIngredient4*ingredientCost4 +
#     AmountIngredient5*ingredientCost5 + AmountIngredient6*ingredientCost6 + AmountIngredient7*ingredientCost7 +
#     AmountIngredient8*ingredientCost8 + AmountIngredient9*ingredientCost9"
#     db.execute_query(f"ALTER TABLE recipe ADD costToCraft AS ({ingred_calc})")


if __name__ == '__main__':
    main()
