import psycopg2
import pandas as pd
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta




def database_connect(connection_params):
    """ Establish connection to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        print('\nConnected to PostgreSQL server\n')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn




def extract_data():
    print("Extracting data from CSV files\n")
    
    # Extract Person Data 
    df_person = pd.read_csv (r'Data/BI_assignment_person.csv',skip_blank_lines=True)
    df_person['phone_number'].fillna('NULL', inplace=True)  #replace NaN values with NULL values so that SQL server recognoise it
    
    # Extract Account Data 
    df_account = pd.read_csv (r'Data/BI_assignment_account.csv',skip_blank_lines=True)
    
    # Extract Transaction Data 
    df_transaction = pd.read_csv (r'Data/BI_assignment_transaction.csv',skip_blank_lines=True)
    df_transaction = df_transaction.drop_duplicates(subset=['id_transaction'], keep='first')
    
    
    return df_person,df_account,df_transaction






def get_birth_date(temp_birth_date):
    reg_ex = re.findall("^([0]?[1-9]|[1][0-2])[./-]([0]?[1-9]|[1|2][0-9]|[3][0|1])[./-]([0-9]{4}|[0-9]{2})$", temp_birth_date)
    if(len(reg_ex)==1):
        birth_date  = datetime.strptime(temp_birth_date, '%m/%d/%Y').date()
    else:
        todays_date = datetime.today().strftime('%Y-%m-%d')
        todays_date = datetime.strptime(todays_date, '%Y-%m-%d').date()
        birth_date  = datetime.strptime(temp_birth_date, '%d-%b-%y').date()
        if(birth_date > todays_date):
            birth_date = birth_date - relativedelta(years=100)
            

    return birth_date




def transform_data(conn,df_person,df_account,df_transaction):
    
     print("Transforming data to load \n")
     df_person['country'] = df_person['country'].astype(str).replace('\'', '', regex=True)
     df_person['surname'] = df_person['surname'].astype(str).replace('\'', '', regex=True)
     df_person['name'] = df_person['name'].astype(str).replace('\'', '', regex=True)
     df_person['city'] = df_person['city'].astype(str).replace('\'', '', regex=True)
     df_person['email'] = df_person['email'].astype(str).replace('\'', '', regex=True)
     
     
     
     print("Loading/Inserting Person data in Database ")
     for i in df_person.index:
         
        temp_birth_date = get_birth_date(df_person['birth_date'][i])

        person_insert_query = """
            INSERT into customer(id_person,name,surname,zip,city,country,email,phone_number,birth_date)
            values('%s','%s','%s',%s,'%s','%s','%s',%s,'%s');
            """ % (int(df_person['id_person'][i]),df_person['name'][i],df_person['surname'][i],int(df_person['zip'][i]),
                    df_person['city'][i],df_person['country'][i],df_person['email'][i],df_person['phone_number'][i],temp_birth_date)
        
        load_data(conn, person_insert_query)
     
        
        
        
     print("Loading/Inserting Account data in Database") 
     for i in df_account.index:
        account_insert_query = """
            INSERT into account(id_account,id_person,account_type)
            values(%s,%s,'%s');
            """ % (int(df_account['id_account'][i]),int(df_account['id_person'][i]),df_account['account_type'][i])
            
        load_data(conn, account_insert_query)
     
        
        
     print("Loading/Inserting Transaction data in Database\n")  
     for i in df_transaction.index:
        transaction_date = datetime.strptime(df_transaction['transaction_date'][i], '%m/%d/%y').date()
        
        transaction_insert_query = """
            INSERT into transaction(id_transaction,id_account,transaction_type,transaction_date,transaction_amount)
            values(%s,%s,'%s','%s',%s);
            """ % (int(df_transaction['id_transaction'][i]),int(df_transaction['id_account'][i]),df_transaction['transaction_type'][i],transaction_date,float(df_transaction['transaction_amount'][i]))
        
        
        load_data(conn, transaction_insert_query)
        
        
     print("Data has been successfully loaded \n")
        
        
        
  


def load_data(conn, load_req):
     # Load data to  PostgreSQL database server
    cursor = conn.cursor()
    try:
        cursor.execute(load_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()



def create_schemas(conn):
    cursor = conn.cursor()
    
    cursor.execute('CREATE TABLE customer (id_person integer PRIMARY KEY, name varchar(50), surname varchar(50), zip integer, city varchar(50), Country varchar(50), email varchar(60) NOT NULL,phone_number integer, birth_date date)')
    
    cursor.execute('CREATE TABLE account (id_account int PRIMARY KEY, id_person int, account_type varchar(20),FOREIGN KEY (id_person) REFERENCES customer (id_person))')

    cursor.execute('CREATE TABLE transaction (id_transaction int PRIMARY KEY,id_account int, transaction_type varchar(5), transaction_date date,transaction_amount float, FOREIGN KEY (id_account) REFERENCES account (id_account))')
    
    #conn.commit()
    cursor.close()
    
    
    
    
def drop_schemas(conn):
    cursor = conn.cursor()
    
    cursor.execute('DROP TABLE transaction;')
    cursor.execute('DROP TABLE account;')
    cursor.execute('DROP TABLE customer;')

    #conn.commit()
    cursor.close()
    
    

def main():
   
    # parameters to connect PostgreSQL server
    # change database,user,password accordingly
    connection_params = {
        "host"      : "localhost",
        "database"  : "enoteDB",
        "user"      : "rakesh",
        "password"  : "enoteRakesh"
    }
    conn = database_connect(connection_params)
   
    drop_schemas(conn)
    create_schemas(conn)
    
    df_person,df_account,df_transaction = extract_data()
    
    transform_data(conn,df_person,df_account,df_transaction)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
    