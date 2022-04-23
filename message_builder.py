"""
Module for handling all message building functions for FFXIV-Market-Calculator
"""
from datetime import datetime
import pandas as pd

from log_handler import LogHandler


class MessageBuilder:  # pylint: disable=too-few-public-methods
    """
    Class for handling the script message building.

    Attributes:
    -------
    ffxiv_logger : Logger object
        Used for logging functions
    update_time : str
        Data update time

    Methods:
    -------
    message_builder(location, sales_data, no_craft):
        Builds a message into the appropriate format
    """
    def __init__(self, logging_config):
        self.ffxiv_logger = LogHandler.get_logger(__name__, logging_config)
        self.update_time = datetime.now().strftime('%d/%m/%Y %H:%M')
        self.message_id = 0
        self.sql_dict = {
            "data_type": "craft_profit",
            "limit": 20,
            "offset": 0
        }
        self.no_craft = False
        self.gatherable = False
        self.results = ""

    def message_data_builder(self, location_db, sale_velocity):
        """
        Queries the database and builds the message data for console or Discord.

        Parameters:
            location_db : str
                Database for the location
            sale_velocity : int
                Minimum number of sales per day to retrieve
        """
        if self.gatherable:
            self.results = location_db.return_query(
                f'SELECT * FROM ( SELECT name, craft_profit, regular_sale_velocity, ave_cost, '
                f'cost_to_craft FROM item WHERE gatherable LIKE "{self.gatherable}" AND '
                f'regular_sale_velocity >= {sale_velocity} AND regular_sale_velocity NOT LIKE '
                f'"NULL" AND regular_sale_velocity IS NOT NULL AND {self.sql_dict["data_type"]} '
                f'NOT LIKE "NULL" AND {self.sql_dict["data_type"]} IS NOT NULL ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]} OFFSET '
                f'{self.sql_dict["offset"]} ) UNION SELECT * FROM ( SELECT name, craft_profit, '
                f'regular_sale_velocity, ave_cost, cost_to_craft FROM item WHERE gatherable LIKE '
                f'"{self.gatherable}" AND regular_sale_velocity >= 0 AND regular_sale_velocity < '
                f'{sale_velocity} AND regular_sale_velocity IS NOT NULL AND regular_sale_velocity '
                f'NOT LIKE "NULL" AND {self.sql_dict["data_type"]} NOT LIKE "NULL" AND '
                f'{self.sql_dict["data_type"]} IS NOT NULL ORDER BY {self.sql_dict["data_type"]} '
                f'DESC LIMIT CASE WHEN {self.sql_dict["limit"]}-( SELECT COUNT(*) FROM item '
                f'WHERE gatherable LIKE "{self.gatherable}" AND regular_sale_velocity >= '
                f'{sale_velocity} AND regular_sale_velocity NOT LIKE "NULL" AND '
                f'regular_sale_velocity IS NOT NULL AND {self.sql_dict["data_type"]} NOT LIKE '
                f'"NULL" AND {self.sql_dict["data_type"]} IS NOT NULL ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]} OFFSET '
                f'{self.sql_dict["offset"]} ) < 0 THEN 0 ELSE {self.sql_dict["limit"]}-('
                f'SELECT COUNT(*) FROM item WHERE gatherable LIKE "{self.gatherable}" AND '
                f'regular_sale_velocity >= {sale_velocity} AND regular_sale_velocity NOT LIKE '
                f'"NULL" AND regular_sale_velocity IS NOT NULL AND {self.sql_dict["data_type"]} '
                f'NOT LIKE "NULL" AND {self.sql_dict["data_type"]} IS NOT NULL ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]} OFFSET '
                f'{self.sql_dict["offset"]} ) END OFFSET {self.sql_dict["offset"]} ) UNION '
                f'SELECT * FROM ( SELECT name, craft_profit, regular_sale_velocity, ave_cost, '
                f'cost_to_craft FROM item WHERE gatherable LIKE "{self.gatherable}" AND '
                f'regular_sale_velocity >= 0 ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'CASE WHEN {self.sql_dict["limit"]}-( SELECT COUNT(*) FROM item WHERE gatherable '
                f'LIKE "{self.gatherable}" AND regular_sale_velocity >= 0 AND '
                f'regular_sale_velocity NOT LIKE "NULL" AND regular_sale_velocity IS NOT NULL AND '
                f'{self.sql_dict["data_type"]} NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'IS NOT NULL ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'{self.sql_dict["limit"]} OFFSET {self.sql_dict["offset"]} ) < 0 THEN 0 ELSE '
                f'{self.sql_dict["limit"]}-( SELECT COUNT(*) FROM item WHERE gatherable LIKE '
                f'"{self.gatherable}" AND regular_sale_velocity >= 0 AND regular_sale_velocity '
                f'NOT LIKE "NULL" AND regular_sale_velocity IS NOT NULL AND '
                f'{self.sql_dict["data_type"]} NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'IS NOT NULL ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'{self.sql_dict["limit"]} OFFSET {self.sql_dict["offset"]} ) END OFFSET '
                f'{self.sql_dict["offset"]} ) ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'{self.sql_dict["limit"]}'
            )

        else:
            self.results = location_db.return_query(
                f'SELECT * FROM (SELECT name, craft_profit, regular_sale_velocity, ave_cost, '
                f'cost_to_craft FROM item WHERE regular_sale_velocity >= {sale_velocity} AND '
                f'regular_sale_velocity NOT LIKE "NULL" AND regular_sale_velocity IS NOT NULL AND '
                f'{self.sql_dict["data_type"]} NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'IS NOT NULL AND item_num IN (SELECT item_result FROM recipe WHERE '
                f'recipe_level_table <= 1000) ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'{self.sql_dict["limit"]} OFFSET {self.sql_dict["offset"]}) UNION '
                f'SELECT * FROM (SELECT name, craft_profit, regular_sale_velocity, ave_cost, '
                f'cost_to_craft FROM item WHERE regular_sale_velocity >= 0 AND '
                f'regular_sale_velocity < {sale_velocity} AND regular_sale_velocity IS NOT NULL '
                f'AND regular_sale_velocity NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'NOT LIKE "NULL" AND {self.sql_dict["data_type"]} IS NOT NULL AND item_num IN '
                f'(SELECT item_result FROM recipe WHERE recipe_level_table <= 1000) ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT CASE WHEN {self.sql_dict["limit"]}-'
                f'(SELECT COUNT(*) FROM item WHERE regular_sale_velocity >= {sale_velocity} AND '
                f'regular_sale_velocity NOT LIKE "NULL" AND regular_sale_velocity IS NOT NULL AND '
                f'{self.sql_dict["data_type"]} NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'IS NOT NULL AND item_num IN (SELECT item_result FROM recipe WHERE '
                f'recipe_level_table <= 1000) ORDER BY ave_cost DESC LIMIT '
                f'{self.sql_dict["limit"]} OFFSET {self.sql_dict["offset"]}) < 0 THEN 0 ELSE '
                f'{self.sql_dict["limit"]}-(SELECT COUNT(*) FROM item WHERE '
                f'regular_sale_velocity >= {sale_velocity} AND regular_sale_velocity NOT LIKE '
                f'"NULL" AND regular_sale_velocity IS NOT NULL AND {self.sql_dict["data_type"]} '
                f'NOT LIKE "NULL" AND {self.sql_dict["data_type"]} IS NOT NULL AND item_num IN '
                f'(SELECT item_result FROM recipe WHERE recipe_level_table <= 1000) ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]} OFFSET '
                f'{self.sql_dict["offset"]}) END OFFSET {self.sql_dict["offset"]}) UNION '
                f'SELECT * FROM (SELECT name, craft_profit, regular_sale_velocity, ave_cost, '
                f'cost_to_craft FROM item WHERE regular_sale_velocity >= 0 ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT CASE WHEN {self.sql_dict["limit"]}-('
                f'SELECT COUNT(*) FROM item WHERE regular_sale_velocity >= 0 AND '
                f'regular_sale_velocity NOT LIKE "NULL" AND regular_sale_velocity IS NOT NULL AND '
                f'{self.sql_dict["data_type"]} NOT LIKE "NULL" AND {self.sql_dict["data_type"]} '
                f'IS NOT NULL AND item_num IN (SELECT item_result FROM recipe WHERE '
                f'recipe_level_table <= 1000) ORDER BY {self.sql_dict["data_type"]} DESC LIMIT '
                f'{self.sql_dict["limit"]} OFFSET {self.sql_dict["offset"]}) < 0 THEN 0 ELSE '
                f'{self.sql_dict["limit"]}-(SELECT COUNT(*) FROM item WHERE '
                f'regular_sale_velocity >= 0 AND regular_sale_velocity NOT LIKE "NULL" AND '
                f'regular_sale_velocity IS NOT NULL AND {self.sql_dict["data_type"]} NOT LIKE '
                f'"NULL" AND {self.sql_dict["data_type"]} IS NOT NULL AND item_num IN (SELECT '
                f'item_result FROM recipe WHERE recipe_level_table <= 1000) ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]} OFFSET '
                f'{self.sql_dict["offset"]}) END OFFSET {self.sql_dict["offset"]}) ORDER BY '
                f'{self.sql_dict["data_type"]} DESC LIMIT {self.sql_dict["limit"]}'
            )

    def message_builder(self, location):
        """
        Takes the data and builds message/s for console or Discord.

        Parameters:
            location : str
                World/DC the sales data is for
        """
        if self.no_craft:
            message_header = (
                f"*(No Craft Cost)* **Data from {location} > 2 avg daily sales @ "
                f"{self.update_time}**\n```"
            )
        elif self.gatherable:
            message_header = (
                f"*(Gatherables)* **Data from {location} > 2 avg daily sales @ "
                f"{self.update_time}**\n```"
            )
        else:
            message_header = (
                f"**Data from {location} > 2 avg daily sales @ "
                f"{self.update_time}**\n```"
            )
        message_footer = "```"
        to_display = ["Name", "Profit", "Avg-Sales", "Avg-Cost", "Avg-Cft-Cost"]
        frame = pd.DataFrame(self.results)
        frame.columns = to_display
        message = message_header + frame.to_string(index=False).replace('"', '') + message_footer
        return self.message_id, message
