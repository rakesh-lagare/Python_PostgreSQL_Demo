# Section B: Practical assignment
I have performed start to end ETL process in python itself. Please follow the steps to know how to run the code
Schema creation is also done in python using psycopg2 package 


## 1. Requirements 
### Tools 
    1. PostgreSQL Server - I have used PostgreSQL server as a database server to store the given CSV file data.
    2. Anaconda (Spyder) – I have used python as programming language for ETL process
### Python Packages 
    psycopg2, Panadas, datetime, re, relativedelta

## 2. Steps to follow (for windows OS) 
   1. Download and install PostgreSQL server from the official PostgreSQL site. During installation process, it asks for admin **username and password**. 
   2. It is important to remember username and password for later usage.
   3. Next run pgAdmin 4 app, a web page will open in internet browser. So now create a database by entering any name for database and admin password.
   4. Download and Install Anaconda application. Also install python packages mentioned above.
   5. Clone my git-repository and open main.py file through spyder application. In main function edit connection_params.
   6. After editing database, user and password parameters run the code.
   7. Now schemas will be created and data will be loaded into respective tables.


## Important Note: 

   1. I have added functions to drop tables and create tables every time code is ran just to avoid constraints error while loading data.

   2. Transaction_ID is added as a primary key in Transaction table. Since Transaction ID is unique, there are lots of redundant transaction IDs in given file. Only first such transaction ID is kept and remaining correspnding rows are removed. So final query result may vary.

## 3. How malformed data is handled 

   1. Handling NaN values - Phone Numbers are not provided in customer csv file. So while inseerting data into database theses NaN valeus are converted into NULL values.
   2. Birth_Date in customer csv file has two types of date formats. And hence are converted into SQL date format before inserting into database.
