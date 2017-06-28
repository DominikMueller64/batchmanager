import logging
import sys
import pkg_resources

def main():
    """Entry point for the application script"""
    pass

# app_logger = logging.getLogger(__name__)
# app_logger.setLevel(logging.DEBUG)

# # Create handlers.
# stream_handler = logging.StreamHandler()
# file_handler = logging.FileHandler(filename=__name__ + '.log', mode='w')
# # level = logging.INFO
# level = logging.DEBUG
# stream_handler.setLevel(level)
# file_handler.setLevel(level)

# # Create formatter.
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # Add formatter to handlers.
# stream_handler.setFormatter(formatter)
# file_handler.setFormatter(formatter)

# # Add handler to logger.
# app_logger.addHandler(stream_handler)
# app_logger.addHandler(file_handler)

# app_logger.info('Start logging.')

msg = r"""
___.           __         .__                                                       
\_ |__ _____ _/  |_  ____ |  |__   _____ _____    ____ _____     ____   ___________ 
 | __ \\__  \\   __\/ ___\|  |  \ /     \\__  \  /    \\__  \   / ___\_/ __ \_  __ \
 | \_\ \/ __ \|  | \  \___|   Y  \  Y Y  \/ __ \|   |  \/ __ \_/ /_/  >  ___/|  | \/
 |___  (____  /__|  \___  >___|  /__|_|  (____  /___|  (____  /\___  / \___  >__|   
     \/     \/          \/     \/      \/     \/     \/     \//_____/      \/       
"""

info = pkg_resources.require("batchmanager")[0]
project_name = info.project_name
version = info.version

print(msg, file=sys.stdout)
print(project_name + ', version ' + version, file=sys.stdout)
print('Dominik Mueller', file=sys.stdout)
