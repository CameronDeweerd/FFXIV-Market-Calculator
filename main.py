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
        "regular_sale_velocity": None,
        "nq_sale_velocity": None,
        "hq_sale_velocity": None,
        "ave_nq_cost": None,
        "ave_hq_cost": None,
        "ave_cost": None
    }

    data, request_response = get_sale_data(item_number, location)
    if request_response.status_code == 404 or not data:
        try:
            FFXIV_LOGGER.info(f"item_number {*item_number,} found no data")
        except TypeError:
            FFXIV_LOGGER.error(f"item_number {item_number} found no data")
        except Exception as err:
            FFXIV_LOGGER.error(f"{err} w/ data pull")
        finally:
            return sales_dict, 0

    try:
        if data["regularSaleVelocity"] > 142 or math.ceil(data["regularSaleVelocity"]) == 0:
            data, request_response = get_sale_data(item_number, location, 10000)
        FFXIV_LOGGER.debug(data)
        sales_dict["regular_sale_velocity"] = round(data["regularSaleVelocity"], 1)
        sales_dict["nq_sale_velocity"] = round(data["nqSaleVelocity"], 1)
        sales_dict["hq_sale_velocity"] = round(data["hqSaleVelocity"], 1)
        if math.ceil(sales_dict["regular_sale_velocity"]) == 0:
            return sales_dict, 1
    except Exception as err:
        FFXIV_LOGGER.debug(data)
        FFXIV_LOGGER.error(f"{err} w/ item_number {item_number}")
        return sales_dict, 0

    sales = data["entries"]
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
        sales_dict["ave_nq_cost"] = None

    try:
        sales_dict["ave_hq_cost"] = int(total_hq_cost / total_hq_sales)
    except ZeroDivisionError:
        sales_dict["ave_hq_cost"] = None

    try:
        sales_dict["ave_cost"] = int(
            (total_nq_cost + total_hq_cost) / (total_nq_sales + total_hq_sales)
        )
    except ZeroDivisionError:
        sales_dict["ave_cost"] = None

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
        f'&statsWithin=14'
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


def profit_table(location_db, location, result_quantity, velocity=10, no_craft=False):
    """
    Print the profit tables to the console.

    Parameters:
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        result_quantity : int
            How many results to print in the output table
        velocity : int
            Minimum sales per day to display
        no_craft : bool
            Whether to also display most profitable without crafting costs
    """
    print("\n\n")
    base_sql = (
        f"SELECT name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft "
        f"FROM item WHERE "
        f"regular_sale_velocity >= {velocity} AND item_num IN ("
        f"SELECT item_result FROM recipe WHERE recipe_level_table <= 1000"
        f") ORDER BY "
    )
    if not no_craft:
        sales_data = location_db.return_query(
            f"{base_sql}craft_profit DESC LIMIT {result_quantity}"
        )
    else:
        sales_data = location_db.return_query(f"{base_sql}ave_cost DESC LIMIT {result_quantity}")

    message = message_builder.message_builder(location, sales_data, no_craft)
    print(message)


def discord_webhook(main_config, discord_config, location_db, location, no_craft=False):
    """
    Function for sending the results to a Discord Webhook.

    Parameters:
        main_config : dict
            Main configuration values
        discord_config : dict
            Discord configuration value
        location_db : SqlManager
            Database object for performing SQL queries
        location : str
            World/DC Location to pull
        no_craft : bool
            Whether to also display most profitable without crafting costs
    """
    discord = DiscordHandler(logging_config)
    base_sql = (
        f"SELECT name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft "
        f"FROM item "
        f"WHERE regular_sale_velocity >= {main_config['min_avg_sales_per_day']} AND "
        f"item_num IN ("
        f"SELECT item_result FROM recipe WHERE recipe_level_table <= 1000"
        f") ORDER BY "
    )
    limit = 20
    offset = 0

    if len(discord_config['message_ids']) == 0:
        if not no_craft:
            sales_data = location_db.return_query(f"{base_sql}craft_profit DESC LIMIT 20")
        else:
            sales_data = location_db.return_query(f"{base_sql}ave_cost DESC LIMIT 20")
        message = message_builder.message_builder(location, sales_data, no_craft)
        discord.discord_message_create(message)
    elif not no_craft:
        for message_id in discord_config['message_ids']:
            sales_data = location_db.return_query(
                f"{base_sql}craft_profit DESC LIMIT {limit} OFFSET {offset}"
            )
            message = message_builder.message_builder(location, sales_data, no_craft)
            discord.discord_message_update(message_id, message)
            offset += 20
    elif no_craft:
        if len(discord_config['message_ids']) > 1:
            message_ids_middle = len(discord_config['message_ids']) // 2
            for message_id in discord_config['message_ids'][:message_ids_middle]:
                sales_data = location_db.return_query(
                    f"{base_sql}craft_profit DESC LIMIT {limit} OFFSET {offset}"
                )
                message = message_builder.message_builder(location, sales_data, no_craft)
                discord.discord_message_update(message_id, message)
                offset += 20

            offset = 0
            for message_id in discord_config['message_ids'][message_ids_middle:]:
                sales_data = location_db.return_query(
                    f"{base_sql}ave_cost DESC LIMIT {limit} OFFSET {offset}"
                )
                message = message_builder.message_builder(location, sales_data, no_craft)
                discord.discord_message_update(message_id, message)
                offset += 20

        elif len(discord_config['message_ids']) == 1:
            for message_id in discord_config['message_ids']:
                sales_data = location_db.return_query(
                    f"{base_sql}craft_profit DESC LIMIT {limit} OFFSET {offset}"
                )
                message = message_builder.message_builder(location, sales_data, no_craft)
                discord.discord_message_update(message_id, message)
                offset += 20

            sales_data = location_db.return_query(
                f"{base_sql}ave_cost DESC LIMIT {limit}"
            )
            message = message_builder.message_builder(location, sales_data, no_craft)
            discord.discord_message_create(message)


def main():
    """Main function"""
    main_config = config.parse_main_config()
    marketboard_type = main_config["marketboard_type"]
    result_quantity = int(main_config["result_quantity"])
    update_quantity = int(main_config["update_quantity"])
    min_avg_sales_per_day = main_config["min_avg_sales_per_day"]
    display_without_craft_cost = main_config["display_without_craft_cost"]
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

    profit_table(location_db, location, result_quantity, min_avg_sales_per_day)
    if display_without_craft_cost:
        profit_table(location_db, location, result_quantity,
                     min_avg_sales_per_day, no_craft=display_without_craft_cost)

    discord_config = config.parse_discord_config()
    if discord_config['discord_enable']:
        discord_webhook(main_config, discord_config, location_db,
                        location, no_craft=display_without_craft_cost)
    else:
        FFXIV_LOGGER.info('Discord Disabled in Config')


if __name__ == '__main__':
    main()
