"""Main module for FFXIV-Market-Calculator"""
import json
import math
import os
import threading
import time

import requests

from message_builder import MessageBuilder
from config_handler import ConfigHandler
from discord_handler import DiscordHandler
from ffxiv_db_constructor import FfxivDbCreation as Db_Create
from log_handler import LogHandler
from sql_helpers import SqlManager

global_db_path = os.path.join("databases", "global_db")
try:
    Db_Create(global_db_path)
    print("New Global DB Created")
except ValueError:
    print("Global Database already exists")
global_db = SqlManager(global_db_path)
config = ConfigHandler('config.ini', global_db)
logging_config = config.parse_logging_config()
message_builder = MessageBuilder(logging_config)
FFXIV_LOGGER = LogHandler.get_logger(__name__, logging_config)


def api_delay():
    """Handles Sleep for synchronous api calls"""
    time.sleep(0.07)  # API only allows 20 checks/sec.


def get_sale_nums(item_number, location):
    """
    Gets the velocity and sale data and creates a dict with it.

    Parameters:
        item_number : str
            The config file path/name
        location : str
            Global database to store shared values
    """
    sales_dict = {
        "regular_sale_velocity": 0,
        "nq_sale_velocity": 0,
        "hq_sale_velocity": 0,
        "ave_nq_cost": 0,
        "ave_hq_cost": 0,
        "ave_cost": 0
    }

    data, request_response = get_sale_data(item_number, location)
    if request_response.status_code == 404 or not data:
        try:
            FFXIV_LOGGER.info(f"item_number {*item_number,} found no data")
        except TypeError:
            FFXIV_LOGGER.error(f"item_number {item_number} found no data")
        except Exception as err:
            FFXIV_LOGGER.error(f"{err} w/ data pull")
        return sales_dict, 0

    try:
        if data["regularSaleVelocity"] > 142 or math.ceil(data["regularSaleVelocity"]) == 0:
            data, request_response = get_sale_data(item_number, location, 10000)
        FFXIV_LOGGER.debug(data)
        sales_dict["regular_sale_velocity"] = round(data["regularSaleVelocity"], 1)
        sales_dict["nq_sale_velocity"] = round(data["nqSaleVelocity"], 1)
        sales_dict["hq_sale_velocity"] = round(data["hqSaleVelocity"], 1)
        if len(data["entries"]) == 0 and math.ceil(sales_dict["regular_sale_velocity"]) == 0:
            return sales_dict, 1
    except Exception as err:
        FFXIV_LOGGER.debug(data)
        FFXIV_LOGGER.error(f"{err} w/ item_number {item_number}")
        return sales_dict, 0

    sales = data["entries"]
    FFXIV_LOGGER.debug(sales_dict)
    sales_dict = sales_calculations(sales_dict, sales)
    return sales_dict, 1


def sales_calculations(sales_dict, sales):
    """
    Performing calculations against the raw sales data.

    Parameters:
        sales_dict : dict
            Stores post-calculated sales data
        sales : dict
            Raw sales data from api
    """
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
                FFXIV_LOGGER.warning(err)
                return sales_dict
        else:
            break

    # get averages and avoid divide by zero
    try:
        sales_dict["ave_nq_cost"] = int(total_nq_cost / total_nq_sales)
    except ZeroDivisionError:
        sales_dict["ave_nq_cost"] = 0

    try:
        sales_dict["ave_hq_cost"] = int(total_hq_cost / total_hq_sales)
    except ZeroDivisionError:
        sales_dict["ave_hq_cost"] = 0

    try:
        sales_dict["ave_cost"] = int(
            (total_nq_cost + total_hq_cost) / (total_nq_sales + total_hq_sales)
        )
    except ZeroDivisionError:
        sales_dict["ave_cost"] = 0

    return sales_dict


def get_sale_data(item_number, location, entries=5000):
    """
    Calls the Universalis API and returns the data and the status code.

    Parameters:
        item_number : str
            Item number to pull sales data for
        location : str
            World/DC Location to pull
        entries : int
            How many Universalis market sale entries to retrieve
    """
    request_response = requests.get(
        f'https://universalis.app/api/v2/history/{location}/{item_number}'
        f'?entriesToReturn={entries}'
    )
    try:
        data = json.loads(request_response.content.decode('utf-8'))
        return data, request_response
    except Exception as err:
        FFXIV_LOGGER.error(err)
        return None, request_response


def update_from_api(location_db, location, start_id, update_quantity):
    """
    Main bridge between pulling the sales data and storing it in the database.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        start_id : str
            Which item ID to start the sequential update from
        update_quantity : int
            How many items to refresh from the API
    """
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
            FFXIV_LOGGER.info(f"item_number {*item_number,} queued for update")
            if item_number[0] == last_item:
                global_db.execute_query(
                    f'UPDATE state SET last_id = 0 WHERE location LIKE "{location}"'
                )
            else:
                global_db.execute_query(
                    f'UPDATE state SET last_id = %i WHERE location LIKE "{location}"' % item_number
                )
        api_delay_thread.join()
    FFXIV_LOGGER.debug(update_list)
    location_db.execute_query_many("UPDATE item SET regular_sale_velocity = ?, "
                                   "nq_sale_velocity = ?, hq_sale_velocity = ?, ave_nq_cost = ?, "
                                   "ave_hq_cost = ?, ave_cost = ? "
                                   "WHERE item_num = ?", update_list)


def update_ingredient_costs(location_db):
    """
    Takes the sales data and updates any crafting ingredient costs.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
    """
    for i in range(10):
        update_list = []
        numbers = location_db.return_query(f"SELECT number, item_ingredient_{i} FROM recipe")
        for num in numbers:
            ingredient_i_id = num[1]
            if not ingredient_i_id == 0:
                ave_cost = location_db.return_query(
                    f"SELECT ave_cost FROM item WHERE item_num = {ingredient_i_id}"
                )
                try:
                    ave_cost = ave_cost[0][0]
                except IndexError:
                    ave_cost = 9999999
                if ave_cost == "None":
                    ave_cost = 9999999
                elif ave_cost <= 0:
                    ave_cost = 9999999
            else:
                ave_cost = 0
            update_list.append(tuple((ave_cost, num[0])))
        location_db.execute_query_many(
            f"UPDATE recipe SET ingredient_cost_{i} = ? WHERE number = ?", update_list
        )
        FFXIV_LOGGER.info(f"ingredient_cost_{i} updated")


def update_cost_to_craft(location_db):
    """
    Takes the ingredient costs and calculates the item crafting cost.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
    """
    FFXIV_LOGGER.info("Updating Cost to Craft")
    location_db.execute_query("UPDATE item "
                              "SET cost_to_craft = (SELECT recipe.cost_to_craft FROM recipe "
                              "WHERE recipe.item_result = item.item_num "
                              "ORDER BY recipe.cost_to_craft DESC LIMIT 1) "
                              "WHERE item_num IN (SELECT recipe.item_result "
                              "FROM recipe WHERE recipe.item_result = item.item_num)"
                              )
    FFXIV_LOGGER.info("Cost to Craft Updated")


def update(location_db, location, start_id, update_quantity):
    """
    Main function to perform all the market cost updating.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        start_id : str
            Which item ID to start the sequential update from
        update_quantity : int
            How many items to refresh from the API
    """
    update_from_api(location_db, location, start_id, update_quantity)
    FFXIV_LOGGER.info("Sales Data Added to Database")
    print("Sales Data Added to Database")
    update_ingredient_costs(location_db)
    FFXIV_LOGGER.info("Ingredient Costs Updated")
    print("Ingredient Costs Updated")
    update_cost_to_craft(location_db)
    FFXIV_LOGGER.info("Cost to Craft Updated")
    print("Cost to Craft Updated")


# def profit_table(location_db, location, result_quantity, extra_tables, velocity=10):
def profit_table(location_db, location, main_config):
    """
    Print the profit tables to the console.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        main_config : dict
            Main configuration values
    """
    print("\n\n")
    message_data = MessageBuilder(logging_config)
    message_data.sql_dict["limit"] = main_config["result_quantity"]
    message_data.message_data_builder(location_db)
    message = message_data.message_builder(location)[1]
    print(message.replace("```", ""))
    if main_config["extra_tables"]["display_without_craft_cost"]:
        message_data = MessageBuilder(logging_config)
        message_data.sql_dict["data_type"] = "raw_profit_per_day"
        message_data.no_craft = main_config["extra_tables"]["display_without_craft_cost"]
        message_data.sql_dict["limit"] = main_config["result_quantity"]
        message_data.message_data_builder(location_db)
        message = message_data.message_builder(location)[1]
        print(message.replace("```", ""))
    if main_config["extra_tables"]["gathering_profit_table"]:
        message_data = MessageBuilder(logging_config)
        message_data.sql_dict["data_type"] = "raw_profit_per_day"
        message_data.gatherable = main_config["extra_tables"]["gathering_profit_table"]
        message_data.sql_dict["limit"] = main_config["result_quantity"]
        message_data.message_data_builder(location_db)
        message = message_data.message_builder(location)[1]
        print(message.replace("```", ""))


def discord_webhook(discord_config, location_db, location, extra_tables):
    """
    Function for sending the results to a Discord Webhook.

    Parameters:
        discord_config : dict
            Discord configuration value
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        extra_tables : dict
            Dictionary of bools, whether to include extra profit tables
    """
    offset = 0
    message_data_queue = []
    if len(discord_config['default_message_ids']) == 0:
        message_data = MessageBuilder(logging_config)
        message_data_queue.append(message_data)
    else:
        offset = 0
        for message_id in discord_config['default_message_ids']:
            message_data = MessageBuilder(logging_config)
            message_data.message_id = message_id
            message_data.sql_dict["offset"] = offset
            message_data_queue.append(message_data)
            offset += 20

    if extra_tables["display_without_craft_cost"] and len(
            discord_config['no_craft_message_ids']) == 0:
        message_data = MessageBuilder(logging_config)
        message_data.sql_dict["data_type"] = "raw_profit_per_day"
        message_data.no_craft = extra_tables["display_without_craft_cost"]
        message_data_queue.append(message_data)
    elif extra_tables["display_without_craft_cost"]:
        offset = 0
        for message_id in discord_config['no_craft_message_ids']:
            message_data = MessageBuilder(logging_config)
            message_data.message_id = message_id
            message_data.sql_dict["offset"] = offset
            message_data.sql_dict["data_type"] = "raw_profit_per_day"
            message_data.no_craft = extra_tables["display_without_craft_cost"]
            message_data_queue.append(message_data)
            offset += 20

    if extra_tables["gathering_profit_table"] and len(
            discord_config['gatherable_message_ids']) == 0:
        message_data = MessageBuilder(logging_config)
        message_data.sql_dict["data_type"] = "raw_profit_per_day"
        message_data.gatherable = extra_tables["gathering_profit_table"]
        message_data_queue.append(message_data)
    elif extra_tables["gathering_profit_table"]:
        offset = 0
        for message_id in discord_config['gatherable_message_ids']:
            message_data = MessageBuilder(logging_config)
            message_data.message_id = message_id
            message_data.sql_dict["offset"] = offset
            message_data.sql_dict["data_type"] = "raw_profit_per_day"
            message_data.gatherable = extra_tables["gathering_profit_table"]
            message_data_queue.append(message_data)
            offset += 20

    discord = DiscordHandler(logging_config)
    for message_data in message_data_queue:
        message_data.message_data_builder(location_db)
        discord.discord_queue_handler(tuple((message_data.message_builder(location))))


def main():
    """Main function"""
    main_config = config.parse_main_config()
    endless_loop = main_config["endless_loop"]
    marketboard_type = main_config["marketboard_type"]
    update_quantity = int(main_config["update_quantity"])
    extra_tables = main_config["extra_tables"]
    location_switch = {
        "World": main_config["world"],
        "Datacentre": main_config["datacentre"],
        "Datacenter": main_config["datacentre"]
    }
    location = location_switch.get(marketboard_type, 'World')
    market_db_name = os.path.join("databases", marketboard_type + "_" + location)

    try:
        Db_Create(market_db_name)
        FFXIV_LOGGER.info("New World or DC database created")
    except ValueError:
        FFXIV_LOGGER.info("World or DC Database already exists")

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

    profit_table(location_db, location, main_config)

    discord_config = config.parse_discord_config()
    if discord_config['discord_enable']:
        discord_webhook(discord_config, location_db,
                        location, extra_tables)
    else:
        FFXIV_LOGGER.info('Discord Disabled in Config')

    FFXIV_LOGGER.info("End of loop")
    return endless_loop


if __name__ == '__main__':
    loop = main()
    while loop:
        FFXIV_LOGGER.info("Sleeping 10-minutes before next loop begins")
        time.sleep(300)
        loop = main()
