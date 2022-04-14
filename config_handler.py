"""
Module for handling all config functions for FFXIV-Market-Calculator
"""
import ast
import configparser
import os
import logging
import pathlib


class ConfigHandler:
    """
    Class for handling the script configuration.

    Attributes:
    -------
    parser : ConfigParser object
        ConfigParser object for use with config manipulation
    configfile : str
        The config file path/name
    global_db : SqlManager object
        Global database to store shared values
    log_handler : Logging object
        Used before logging config is loaded
    ffxiv_logger : Logger object
        Used for logging functions
    config : dict
        Dictionary to store the loaded configurations

    Methods:
    -------
    config_create():
        Creates a new config file with all default options
    parse_main_config():
        Retrieves main values from config file
    parse_logging_config():
        Retrieves logging values from config file
    parse_discord_config():
        Retrieves discord values from config file
    main_validation():
        Validates the main config values for correct values/types
    logging_validation():
        Validates the logging config values for correct values/types
    discord_validation():
        Validates thee discord config values for correct values/types
    get_config_parser():
        Unsure, this could potentially be blown away as I cannot find a usage in project
    """
    parser = configparser.ConfigParser()

    def __init__(self, configfile, global_db):
        """
        Constructs all the necessary attributes for the ConfigParser object
        and creates a new config file if none exists.

        Parameters:
            configfile : str
                The config file path/name
            global_db : SqlManager
                Global database to store shared values
        """
        self.parser = configparser.ConfigParser()
        self.configfile = configfile
        self.global_db = global_db
        self.log_handler = logging
        self.ffxiv_logger = self.log_handler.getLogger(__name__)
        self.config = {}
        if not os.path.exists(pathlib.Path(__file__).parent / self.configfile):
            self.ffxiv_logger.info("Config File does not exist, creating one with default values")
            self.config_create()

    def config_create(self):
        """
        Write a new default config file if none exists
        """
        self.parser.add_section('MAIN')
        self.parser['MAIN']['MarketboardType'] = 'World'
        self.parser['MAIN']['Datacentre'] = 'Crystal'
        self.parser['MAIN']['World'] = 'Zalera'
        self.parser['MAIN']['ResultQuantity'] = '50'
        self.parser['MAIN']['UpdateQuantity'] = '0'
        self.parser['MAIN']['MinAvgSalesPerDay'] = '20'
        self.parser["MAIN"]['DisplayWithoutCraftCost'] = 'False'

        self.parser.add_section('LOGGING')
        self.parser['LOGGING']['LogEnable'] = 'True'
        self.parser['LOGGING']['LogLevel'] = 'INFO'
        self.parser['LOGGING']['LogMode'] = 'WRITE'
        self.parser['LOGGING']['LogFile'] = 'ffxiv_market_calculator.log'

        self.parser.add_section('DISCORD')
        self.parser['DISCORD']['DiscordEnable'] = 'False'
        self.parser['DISCORD']['MessageIds'] = '[123456789123456789,123456789123456789]'

        with open(self.configfile, 'w', encoding='utf-8') as configfile:
            self.parser.write(configfile)

    def parse_main_config(self):
        """
        Retrieves main values from config file
        """
        self.parser.read(self.configfile)
        try:
            self.config = {
                "marketboard_type": self.parser["MAIN"].get(
                    'MarketboardType', 'World'
                ).capitalize(),
                "datacentre": self.parser["MAIN"].get('Datacentre', 'Crystal').capitalize(),
                "world": self.parser["MAIN"].get('World', 'Zalera').capitalize(),
                "result_quantity": self.parser["MAIN"].getint('ResultQuantity', 50),
                "update_quantity": self.parser["MAIN"].getint(
                    'UpdateQuantity', 0
                ),
                "min_avg_sales_per_day": self.parser["MAIN"].getint('MinAvgSalesPerDay', 20),
                "display_without_craft_cost": self.parser["MAIN"].getboolean(
                    'DisplayWithoutCraftCost', False)
            }
        except Exception as err:
            self.ffxiv_logger.error("MAIN Config was invalid, setting back to defaults: %i", {err})

            self.parser["MAIN"]['MarketboardType'] = 'World'.capitalize()
            self.parser["MAIN"]['Datacentre'] = 'Crystal'.capitalize()
            self.parser["MAIN"]['World'] = 'Zalera'.capitalize()
            self.parser["MAIN"]['ResultQuantity'] = '50'
            self.parser["MAIN"]['UpdateQuantity'] = '0'
            self.parser["MAIN"]['MinAvgSalesPerDay'] = '20'
            self.parser["MAIN"]['DisplayWithoutCraftCost'] = 'False'
            with open(self.configfile, 'w', encoding='utf-8') as configfile:
                self.parser.write(configfile)

            self.config = {
                "marketboard_type": 'World',
                "datacentre": 'Crystal',
                "world": 'Zalera',
                "result_quantity": 50,
                "update_quantity": 0,
                "min_avg_sales_per_day": 20,
                "display_without_craft_cost": False
            }
        self.main_validation()
        self.ffxiv_logger.info("Main Config Loaded")
        return self.config

    def parse_logging_config(self):
        """
        Retrieves logging values from config file
        """
        self.ffxiv_logger.info("Loading Logging Config")
        self.parser.read(self.configfile)
        try:
            self.config = {
                "log_enable": self.parser["LOGGING"].getboolean('LogEnable', False),
                "log_level": self.parser['LOGGING'].get('LogLevel', 'INFO'),
                "log_mode": self.parser['LOGGING'].get('LogMode', 'Write'),
                "log_file": self.parser['LOGGING'].get('LogFile', 'ffxiv_market_calculator.log'),
            }
        except Exception as err:
            self.ffxiv_logger.error(
                "LOGGING Config was invalid, setting back to defaults: %i",{err}
            )
            self.parser['LOGGING']['LogEnable'] = 'True'
            self.parser['LOGGING']['LogLevel'] = 'INFO'
            self.parser['LOGGING']['LogMode'] = 'WRITE'
            self.parser['LOGGING']['LogFile'] = 'ffxiv_market_calculator.log'
            with open(self.configfile, 'w', encoding='utf-8') as configfile:
                self.parser.write(configfile)

            self.config = {
                "log_enable": False,
                "log_level": 'INFO',
                "log_mode": 'Write',
                "log_file": 'ffxiv_market_calculator.log'
            }
        self.logging_validation()
        return self.config

    def parse_discord_config(self):
        """
        Retrieves discord values from config file
        """
        self.ffxiv_logger.info("Loading Discord Config")
        self.parser.read(self.configfile)
        try:
            self.config = {
                "discord_enable": self.parser["DISCORD"].getboolean('DiscordEnable', False),
                "message_ids": ast.literal_eval(self.parser['DISCORD'].get('MessageIds'))
            }
        except Exception as err:
            self.ffxiv_logger.error(
                "DISCORD Config was invalid, setting back to defaults: %i", {err}
            )
            self.parser["DISCORD"]['DiscordEnable'] = 'False'
            self.parser['DISCORD']['MessageIds'] = '[123456789123456789, 123456789123456789]'
            with open(self.configfile, 'w', encoding='utf-8') as configfile:
                self.parser.write(configfile)

            self.config = {
                "discord_enable": False,
                "message_ids": []
            }
        self.discord_validation()
        self.ffxiv_logger.info("Loaded Discord Config")
        return self.config

    def main_validation(self):
        """
        Validates the main config values for correct values/types
        """
        self.ffxiv_logger.info("Performing Main Config Validation")
        datacentre_data = self.global_db.return_query('SELECT name FROM datacentre')
        world_data = self.global_db.return_query('SELECT name FROM world')
        valid_datacentres = valid_worlds = [] = []
        for datacentre in datacentre_data:
            valid_datacentres.append(datacentre[0])
        for world in world_data:
            valid_worlds.append(world[0])

        type_check = all([
            isinstance(self.config["result_quantity"], int),
            isinstance(self.config["update_quantity"], int),
            isinstance(self.config["min_avg_sales_per_day"], int),
            isinstance(self.config["display_without_craft_cost"], bool)
        ])
        value_check = all([
            self.config["marketboard_type"] in ["World", "Datacentre", "Datacenter"],
            self.config["datacentre"] in valid_datacentres,
            self.config["world"] in valid_worlds,
            self.config["result_quantity"] > 0,
            self.config["min_avg_sales_per_day"] > 0
        ])
        if not type_check and value_check:
            self.ffxiv_logger.error(
                "Main Config Validation FAILED on both Type and Value validations"
            )
            raise TypeError
        if not type_check:
            self.ffxiv_logger.error("Main Config Validation FAILED on Type validations")
            raise TypeError
        if not value_check:
            self.ffxiv_logger.error("Main Config Validation FAILED on Value validations")
            raise ValueError
        self.ffxiv_logger.info("Main Config Validation Complete")

    def logging_validation(self):
        """
        Validates the logging config values for correct values/types
        """
        self.ffxiv_logger.info("Performing Logging Config Validation")
        type_check = all([
            isinstance(self.config["log_level"], str),
            isinstance(self.config["log_mode"], str),
            isinstance(self.config["log_file"], str)
        ])
        value_check = all([
            self.config["log_level"] in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
            self.config["log_mode"] in ["WRITE", "APPEND"]
        ])
        if not isinstance(self.config["log_enable"], bool):
            raise ValueError
        if self.config["log_enable"] and not type_check and not value_check:
            self.ffxiv_logger.error(
                "Logging Config Validation FAILED on both Type and Value validations"
            )
            raise TypeError
        if self.config["log_enable"] and not type_check:
            self.ffxiv_logger.error("Logging Config Validation FAILED on Type validations")
            raise TypeError
        if self.config["log_enable"] and not value_check:
            self.ffxiv_logger.error("Logging Config Validation FAILED on Value validations")
            raise ValueError
        self.ffxiv_logger.info("Logging Config Validation Complete")

    def discord_validation(self):
        """
        Validates the discord config values for correct values/types
        """
        self.ffxiv_logger.info("Performing Discord Config Validation")
        if not isinstance(self.config["discord_enable"], bool):
            self.ffxiv_logger.error("Discord Config Validation FAILED on Type validation")
            raise TypeError
        self.ffxiv_logger.info("Discord Config Validation Complete")

    def get_config_parser(self):
        """
        Unsure, this could potentially be blown away as I cannot find a usage in project
        """
        return self.parser.read(self.configfile), self.configfile
