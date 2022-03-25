import configparser
import json
import logging
import os
import pandas as pd
import requests
import sys
import threading
import time

from FFXIV_DB_constructor import FfxivDbCreation as Db_Create
from logging.handlers import RotatingFileHandler
from SQLhelpers import SqlManager

t0 = time.time()
ffxiv_logger = logging.Logger


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


def load_config():
    cfg = configparser.ConfigParser()
    cfg.read("config.ini")
    try:
        config_dict = {
            "marketboard_type": cfg["MAIN"].get('MarketboardType', 'World').capitalize(),
            "datacentre": cfg["MAIN"].get('Datacentre', 'Crystal').capitalize(),
            "world": cfg["MAIN"].get('World', 'Zalera').capitalize(),
            "result_quantity": cfg["MAIN"].getint('ResultQuantity', 50),
            "update_quantity": cfg["MAIN"].getint('UpdateQuantity', 0),
            "min_avg_sales_per_day": cfg["MAIN"].getint('MinAvgSalesPerDay', 20),
            "log_enable": cfg["LOGGING"].getboolean('LogEnable', False),
            "log_level": cfg['LOGGING'].get('LogLevel', 'INFO'),
            "log_mode": cfg['LOGGING'].get('LogMode', 'Write'),
            "log_file": cfg['LOGGING'].get('LogFile', 'ffxiv_market_calculator.log')
        }
        return config_dict
    except Exception as err:
        print(f"Config Load Error: {err}")
        exit()


def config_validation(config_dict, global_db):
    error_list = []
    if config_dict["marketboard_type"] not in ["World", "Datacentre", "Datacenter"]:
        error_list.append("Config Error: Marketboard Type is Unknown, this value should be World, Datacentre, or Datacenter")

    datacentre_data = global_db.return_query(f'SELECT name FROM datacentre')
    valid_datacentres = []
    for item in datacentre_data:
        valid_datacentres.append(item[0])
    if config_dict["datacentre"] not in valid_datacentres:
        error_list.append("Config Error: Datacentre is not in Database, check config")

    world_data = global_db.return_query(f'SELECT name FROM world')
    valid_worlds = []
    for item in world_data:
        valid_worlds.append(item[0])
    if config_dict["world"] not in valid_worlds:
        error_list.append("Config Error: World is not in Database, check config")

    if not isinstance(config_dict["result_quantity"], int) or config_dict["result_quantity"] == 0:
        error_list.append("Config Error: Result Quantity must be a whole number and greater than 0")

    if not isinstance(config_dict["update_quantity"], int):
        error_list.append("Config Error: Update Quantity must be a whole number")

    if not isinstance(config_dict["min_avg_sales_per_day"], int) or config_dict["min_avg_sales_per_day"] == 0:
        error_list.append("Config Error: Result Quantity must be a whole number and greater than 0")

    if not isinstance(config_dict["log_enable"], bool):
        error_list.append("Config Error: Log Enable must be True or False")

    if config_dict["log_level"] not in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]:
        error_list.append("Config Error: Log Level is not known, must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG")

    if config_dict["log_mode"] not in ["WRITE", "APPEND"]:
        error_list.append("Config Error: Marketboard Type is Unknown, this value should be "
                          "World, Datacentre, or Datacenter")

    if len(error_list) > 0:
        for error in error_list:
            print(error)
        exit()


def config_logging(config_dict):
    global ffxiv_logger
    if config_dict['log_enable']:
        if config_dict['log_mode'] == "WRITE":
            log_mode = 'w'
        else:
            log_mode = 'a'
        logging.basicConfig(
            level=logging.getLevelName(config_dict['log_level']),
            format='%(asctime)s\t%(levelname)s\t\t%(name)s\t\t%(message)s',
            filename=config_dict['log_file'],
            filemode=log_mode
        )
    ffxiv_logger = logging.getLogger('ffxiv-market-calc')


def api_delay():
    time.sleep(0.06)  # API only allows 7 checks/sec.


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
        if regular_sale_velocity == 0:
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
def update_from_api(location_db, global_db, location, start_id, update_quantity):
    table_name = "item"
    if update_quantity == 0:
        query = f"SELECT item_num FROM item WHERE item_num >= {start_id}"
    else:
        query = f"SELECT item_num FROM item WHERE item_num >= {start_id} " \
                f"ORDER BY item_num ASC LIMIT {update_quantity}"
    data = location_db.return_query(query)

    for item_number in data:
        api_delay_thread = threading.Thread(target=api_delay)
        api_delay_thread.start()
        dictionary, success = get_sale_nums(*item_number, location)
        if success == 1:
            query = "UPDATE `{}` SET {} WHERE item_num = %s" % item_number
            new_data_value = ', '.join(
                ['`{}`="{}"'.format(column_name, value) for column_name, value in dictionary.items()])
            q = query.format(table_name, new_data_value)
            location_db.execute_query(q)
            ffxiv_logger.info(f"item_number {*item_number,} updated")
            update_state_query = f'UPDATE state SET last_id = %i WHERE location LIKE "{location}"' % item_number
            global_db.execute_query(update_state_query)
        api_delay_thread.join()


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
def update(location_db, global_db, location, start_id, update_quantity):
    update_from_api(location_db, global_db, location, start_id, update_quantity)
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
    to_display = "name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft"
    level_limited_recipes = f"SELECT item_result FROM recipe WHERE recipe_level_table <={recipe_lvl}"
    x = location_db.return_query(
        f"SELECT {to_display} FROM item WHERE regular_sale_velocity >= {velocity} AND item_num "
        f"IN ({level_limited_recipes}) ORDER BY craft_profit DESC LIMIT {result_quantity}")

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
    config_logging(config_dict)
    if config_dict['log_enable']:
        print("Config loaded with logging enabled, logging to file from now")
    sys.stderr = StreamToLogger(ffxiv_logger, logging.ERROR)

    marketboard_type = config_dict["marketboard_type"]
    min_avg_sales_per_day = config_dict["min_avg_sales_per_day"]

    if marketboard_type == "World":
        location = config_dict["world"]
    elif marketboard_type == "Datacentre" or marketboard_type == "Datacenter":
        location = config_dict["datacentre"]
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
    update_quantity = int(config_dict["update_quantity"])
    update(location_db, global_db, location, start_id, update_quantity)
    if update_quantity == 0:
        global_db.execute_query(
            f'UPDATE state SET last_id = 0 WHERE'
            f'marketboard_type LIKE "{marketboard_type}" AND location LIKE "{location}'
        )
    result_quantity = int(config_dict["result_quantity"])
    profit_table(location_db, market_db_name, result_quantity, min_avg_sales_per_day)
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
