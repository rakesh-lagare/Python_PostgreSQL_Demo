# PostgreSQL Demo
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

 
 ## DEMO SQL query to generate the report
            SELECT c.id_person,date_trunc('month', t.transaction_date) as month , sum(transaction_amount) as sum_of_transactions
            FROM transaction t,customer c,account a
            WHERE t.id_account = a.id_account and a.id_person = c.id_person and c.id_person in (345,1234)
            and (t.transaction_date Between '2020-02-15' AND '2020-06-06')
            GROUP BY  month,c.id_person
            ORDER BY c.id_person,month DESC;
 
  
