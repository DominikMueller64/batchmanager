import logging

def main():
    """Entry point for the application script"""
    pass

app_logger = logging.getLogger(__name__)
app_logger.setLevel(logging.DEBUG)

# Create handlers.
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(filename=__name__ + '.log', mode='w')
# level = logging.INFO
level = logging.DEBUG
stream_handler.setLevel(level)
file_handler.setLevel(level)

# Create formatter.
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add formatter to handlers.
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handler to logger.
app_logger.addHandler(stream_handler)
app_logger.addHandler(file_handler)

app_logger.info('Start logging.')
print("Welcome")

