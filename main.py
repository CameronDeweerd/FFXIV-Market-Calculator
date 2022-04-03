import json
import math
import os
import threading
import time
from datetime import datetime

import pandas as pd
import requests

from ConfigHandler import ConfigHandler
from DiscordHandler import DiscordHandler
from FFXIV_DB_constructor import FfxivDbCreation as Db_Create
from LogHandler import LogHandler
from SQLhelpers import SqlManager

global_db_path = os.path.join("databases", "global_db")
try:
    Db_Create(global_db_path)
    print("New Global DB Created")
except ValueError:
    print("Global Database already exists")
    pass
global_db = SqlManager(global_db_path)
config = ConfigHandler('config.ini', global_db)
logging_config = config.parse_logging_config()
ffxiv_logger = LogHandler.get_logger(__name__, logging_config)


def api_delay():
    time.sleep(0.06)  # API only allows 20 checks/sec.


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
        try:
            ffxiv_logger.info(f"item_number {*item_number,} found no data")
        except TypeError:
            ffxiv_logger.error(f"item_number {item_number} found no data")
        except Exception as err:
            ffxiv_logger.error(f"{err} w/ data pull")
        finally:
            return sale_data, 0

    try:
        if data["regularSaleVelocity"] > 142:
            data, r = get_sale_data(item_number, location, 10000)
        regular_sale_velocity = round(data["regularSaleVelocity"], 1)
        nq_sale_velocity = round(data["nqSaleVelocity"], 1)
        hq_sale_velocity = round(data["hqSaleVelocity"], 1)
        if math.ceil(regular_sale_velocity) == 0:
            return sale_data, 1
    except Exception as err:
        ffxiv_logger.error(f"{err} w/ item_number {item_number}")
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
            except Exception as err:
                ffxiv_logger.warning(err)
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
def get_sale_data(item_number, location, entries=5000):
    r = requests.get(f'https://universalis.app/api/history/{location}/{item_number}?entries={entries}')
    try:
        data = json.loads(r.content.decode('utf-8'))
        return data, r
    except Exception as err:
        ffxiv_logger.error(err)
        return None, r


# puts the data from the Universalis API into the db
def update_from_api(location_db, location, start_id, update_quantity):
    last_id = location_db.return_query('SELECT item_num FROM item ORDER BY item_num DESC LIMIT 1')
    last_item = int(last_id[0][0])
    if update_quantity == 0:
        query = f"SELECT item_num FROM item WHERE item_num >= {start_id}"
    else:
        query = f"SELECT item_num FROM item WHERE item_num >= {start_id} " \
                f"ORDER BY item_num ASC LIMIT {update_quantity}"
    data = location_db.return_query(query)

    update_list = []
    for item_number in data:
        api_delay_thread = threading.Thread(target=api_delay)
        api_delay_thread.start()
        dictionary, success = get_sale_nums(*item_number, location)
        if success == 1:
            dictionary['item_num'] = item_number[0]
            update_list.append(tuple((dictionary.values())))
            ffxiv_logger.info(f"item_number {*item_number,} queued for update")
            if item_number[0] == last_item:
                global_db.execute_query(f'UPDATE state SET last_id = 0 WHERE location LIKE "{location}"')
            else:
                global_db.execute_query(f'UPDATE state SET last_id = %i WHERE location LIKE "{location}"' % item_number)
        api_delay_thread.join()
    location_db.execute_query_many(f"UPDATE item SET regular_sale_velocity = ?, nq_sale_velocity = ?, "
                                   f"hq_sale_velocity = ?, ave_nq_cost = ?, ave_hq_cost = ?, ave_cost = ? "
                                   f"WHERE item_num = ?", update_list)


# updates the ingredientCost values 0-9 for the recipe table
def update_ingredient_costs(location_db):
    for i in range(10):
        update_list = []
        numbers = location_db.return_query(f"SELECT number, item_ingredient_{i} FROM recipe")
        for num in numbers:
            ingredient_i_id = num[1]
            if not ingredient_i_id == 0:
                ave_cost = location_db.return_query(f"SELECT ave_cost FROM item WHERE item_num = {ingredient_i_id}")
                try:
                    ave_cost = ave_cost[0][0]
                except IndexError:
                    ave_cost = 9999999
                if ave_cost == "None":
                    ave_cost = 9999999
            else:
                ave_cost = 0
            update_list.append(tuple((ave_cost, num[0])))
        location_db.execute_query_many(f"UPDATE recipe SET ingredient_cost_{i} = ? WHERE number = ?", update_list)
        ffxiv_logger.info(f"ingredient_cost_{i} updated")


# copies over the cost to craft data from recipes
def update_cost_to_craft(location_db):
    update_cost_change = []
    update_cost_is_ave = []
    numbers = location_db.return_query("SELECT * FROM item")
    for index, num in enumerate(numbers):
        cost = location_db.return_query(f"SELECT cost_to_craft FROM recipe WHERE item_result = {num[0]}")
        try:
            cost = cost[0][0]
            update_cost_change.append(tuple((cost, num[0])))
        except IndexError:
            update_cost_is_ave.append(tuple((num[0],)))
        if index % 100 == 0:
            ffxiv_logger.info(f"{index}/{len(numbers)} queued for update")
    location_db.execute_query_many(f"UPDATE item SET cost_to_craft = ? WHERE item_num = ?", update_cost_change)
    location_db.execute_query_many(f"UPDATE item SET cost_to_craft = ave_cost WHERE item_num = ?", update_cost_is_ave)


# pulls data from the API, and performs all required calculations on it.
def update(location_db, location, start_id, update_quantity):
    update_from_api(location_db, location, start_id, update_quantity)
    ffxiv_logger.info("Sales Data Added to Database")
    print("Sales Data Added to Database")
    update_ingredient_costs(location_db)
    ffxiv_logger.info("Ingredient Costs Updated")
    print("Ingredient Costs Updated")
    update_cost_to_craft(location_db)
    ffxiv_logger.info("Cost to Craft Updated")
    print("Cost to Craft Updated")


def profit_table(location_db, db_name, result_quantity, velocity=10, recipe_lvl=1000):
    print("\n\n")
    to_display = ["Name", "Profit", "Avg-Sales", "Avg-Cost", "Avg-Cft-Cost"]
    to_sql = "name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft"
    level_limited_recipes = f"SELECT item_result FROM recipe WHERE recipe_level_table <={recipe_lvl}"
    x = location_db.return_query(
        f"SELECT {to_sql} FROM item WHERE "
        f"regular_sale_velocity >= {velocity} AND item_num IN ({level_limited_recipes}) "
        f"ORDER BY craft_profit DESC LIMIT {result_quantity}")

    frame = pd.DataFrame(x)
    print(f"             Data from {db_name} w/ {velocity} or more daily sales")
    print(f"        _____________________________________________________________________________")
    frame.style.set_caption("Hello World")
    frame.columns = to_display
    print(frame.to_string(index=False).replace('"', ''))


def discord_webhook(main_config, discord_config, location_db, location):
    discord = DiscordHandler(discord_config, logging_config)
    message_header = f"**Data from {location} > 2 avg daily sales @ {datetime.now().strftime('%d/%m/%Y %H:%M')}**\n```"
    message_footer = f"```"
    to_display = ["Name", "Profit", "Avg-Sales", "Avg-Cost", "Avg-Cft-Cost"]
    to_sql = "name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft"
    limit = 20
    offset = 0
    level_limited_recipes = f"SELECT item_result FROM recipe WHERE recipe_level_table <= 1000"

    if len(discord_config['message_ids']) == 0:
        results = location_db.return_query(
            f"SELECT {to_sql} FROM item WHERE "
            f"regular_sale_velocity >= {main_config['min_avg_sales_per_day']} AND item_num IN "
            f"({level_limited_recipes}) ORDER BY craft_profit DESC LIMIT 20")
        frame = pd.DataFrame(results)
        frame.columns = to_display
        message = message_header + frame.to_string(index=False).replace('"', '') + message_footer
        discord.discord_message_create(message)
    else:
        for message_id in discord_config['message_ids']:
            results = location_db.return_query(
                f"SELECT {to_sql} FROM item WHERE "
                f"regular_sale_velocity >= {main_config['min_avg_sales_per_day']} AND item_num IN "
                f"({level_limited_recipes}) ORDER BY craft_profit DESC LIMIT {limit} OFFSET {offset}")
            frame = pd.DataFrame(results)
            frame.columns = to_display
            message = message_header + frame.to_string(index=False).replace('"', '') + message_footer
            discord.discord_message_update(message_id, message)
            offset += 10


def main():
    main_config = config.parse_main_config()
    marketboard_type = main_config["marketboard_type"]
    result_quantity = int(main_config["result_quantity"])
    update_quantity = int(main_config["update_quantity"])
    min_avg_sales_per_day = main_config["min_avg_sales_per_day"]
    location_switch = {
        "World": main_config["world"],
        "Datacentre": main_config["datacentre"],
        "Datacenter": main_config["datacentre"]
    }
    location = location_switch.get(marketboard_type, 'World')
    market_db_name = os.path.join("databases", marketboard_type + "_" + location)

    try:
        Db_Create(market_db_name)
        ffxiv_logger.info("New World or DC database created")
    except ValueError:
        ffxiv_logger.info("World or DC Database already exists")
        pass

    selected_location_start_id = global_db.return_query(
        f'SELECT last_id FROM state WHERE '
        f'marketboard_type LIKE "{marketboard_type}" AND location LIKE "{location}"'
    )

    if len(selected_location_start_id) > 0:
        start_id = int(selected_location_start_id[0][0])
    else:
        start_id = 0
        global_db.execute_query(
            f'INSERT INTO state (marketboard_type, location, last_id) '
            f'VALUES("{marketboard_type}", "{location}", 0)'
        )
    location_db = SqlManager(market_db_name)

    update(location_db, location, start_id, update_quantity)
    if update_quantity == 0:
        global_db.execute_query(
            f'UPDATE state SET last_id = 0 WHERE '
            f'marketboard_type LIKE "{marketboard_type}" AND location LIKE "{location}"'
        )

    profit_table(location_db, market_db_name, result_quantity, min_avg_sales_per_day)

    discord_config = config.parse_discord_config()
    if discord_config['discord_enable']:
        discord_webhook(main_config, discord_config, location_db, location)
    else:
        ffxiv_logger.info('Discord Disabled in Config')


if __name__ == '__main__':
    main()
