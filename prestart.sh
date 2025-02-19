#!/bin/bash

# - - - - - Update pip - - - - - 
pip install --upgrade pip

# - - - - - Mysql connection - - - - - 
# pip install aiomysql                  # INSERT INTO doesn't work. Don't use.
# pip install asyncmy                   # async mysql-connector. Seems that async doesn't work??
pip install mysql-connector-python      # only synchronys which seems to work

# - - - - - Other mixed - - - - - 
pip install psutil
pip install pandas
pip install pillow
