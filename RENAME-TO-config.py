import os

# REQUIRED FOR SETUP
# You must enter your Robinhood credentials (email and password).
# You must also make sure this file is saved as config.py.
#
# All other global variables you can leave at their defaults and tweak later.

# Enter your Robinhood credentials.
USERNAME = 'email@email.com'
PASSWORD = 'password'

# Interval (in seconds) before the next batch of Robinhood requests are made.
# It is recommended for this value to be aboe 3.
# If you have a lot of open positions and orders, you may want to increase this
# value to > 10.
RH_REQUEST_INTERVAL = 4

# Interval (in seconds) before all the display panes are refreshed.
# If you are using Optionhood over SSH on a slow internet connection
# you might want to increase this value to between 2-5 seconds.
TMUX_REFRESH_INTERVAL = 0.5 

# Specify your preferred submenu prompt for the command pane.
SUBMENU_PROMPT = '>>>> '

# Project root directory.
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
