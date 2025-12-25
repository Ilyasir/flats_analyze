import logging
import sys
import time


class RelativeTimeFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
        self.start_time = time.time()

    def formatTime(self, record, datefmt=None):
        elapsed_seconds = record.created - self.start_time
        return time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))


def setup_logger():
    """
    Настройка логгера: INFO в консоль, DEBUG в файл parser.log.
    Возвращает настроенный объект логгера.
    """
    logger = logging.getLogger("rentcheck")
    
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = RelativeTimeFormatter(
        "[%(asctime)s] %(levelname)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler("parser.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger