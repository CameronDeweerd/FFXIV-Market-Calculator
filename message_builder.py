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
                f"SELECT name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft "
                f"FROM item "
                f"WHERE gatherable = 'True' AND "
                f"regular_sale_velocity >= {sale_velocity} AND "
                f"item_num IN ("
                f"SELECT item_result FROM recipe WHERE recipe_level_table <= 1000"
                f") ORDER BY {self.sql_dict['data_type']} DESC LIMIT {self.sql_dict['limit']} "
                f"OFFSET {self.sql_dict['offset']}"
            )
        else:
            self.results = location_db.return_query(
                f"SELECT name, craft_profit, regular_sale_velocity, ave_cost, cost_to_craft "
                f"FROM item "
                f"WHERE regular_sale_velocity >= {sale_velocity} AND "
                f"item_num IN ("
                f"SELECT item_result FROM recipe WHERE recipe_level_table <= 1000"
                f") ORDER BY {self.sql_dict['data_type']} DESC LIMIT {self.sql_dict['limit']} "
                f"OFFSET {self.sql_dict['offset']}"
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
