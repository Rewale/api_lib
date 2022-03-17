from loguru import logger


# логгер кролика
logger.add('logs/api_rabbit.log',
           format="{time} {level} {message}",
           filter=lambda record: "api_rabbit_log" in record["extra"],
           rotation="1 MB",
           compression="tar.gz")
rabbit_logger = logger.bind(api_rabbit_log=True)
