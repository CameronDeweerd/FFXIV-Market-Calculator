import logging


class LogHandler(object):
    _LOG = None

    @staticmethod
    def __create_logger(module, logging_config):
        """
        A private method that interacts with the python
        logging module
        """
        # Initialize the class variable with logger object
        LogHandler._LOG = logging.getLogger(module)
        if logging_config['log_mode'] == "APPEND":
            log_mode = 'a'
        else:
            log_mode = 'w'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s\t%(levelname)s\t\t%(name)s\t\t%(message)s',
                            datefmt="%Y-%m-%d %H:%M:%S",
                            filename=logging_config['log_file'],
                            filemode=log_mode)

        # set the logging level based on the user selection
        if logging_config['log_level'] == "INFO":
            LogHandler._LOG.setLevel(logging.INFO)
        elif logging_config['log_level'] == "ERROR":
            LogHandler._LOG.setLevel(logging.ERROR)
        elif logging_config['log_level'] == "DEBUG":
            LogHandler._LOG.setLevel(logging.DEBUG)
        return LogHandler._LOG

    @staticmethod
    def get_logger(module, logging_config):
        """
        A static method called by other modules to initialize logger in
        their own module
        """
        logger = LogHandler.__create_logger(module, logging_config)

        # return the logger object
        return logger
