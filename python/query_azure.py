import pyodbc
import configparser
import argparse
import pandas as pd

config = configparser.ConfigParser()
config.read('./config.ini')

server = config['azure']['SERVER']
database = config['azure']['DB']
username = config['azure']['USER']
password = config['azure']['PASS']  

driver= '/opt/homebrew/Cellar/msodbcsql18/18.2.1.1/lib/libmsodbcsql.18.dylib' #'{ODBC Driver 17 for SQL Server}'

parser = argparse.ArgumentParser(description='Upload data to the Azure Database.')
parser.add_argument('-f', dest='filename', metavar='<filename>', 
                        help='CSV file name to read the input data')
parser.add_argument('-n', dest='rownum', metavar='rownum', 
                        help='Number of rows to read from the input file (default: 10)')
# parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                     help='an integer for the accumulator')
# parser.add_argument('--sum', dest='accumulate', action='store_const',
#                     const=sum, default=max,
#                     help='sum the integers (default: find the max)')

args = parser.parse_args()
input_filename = args.filename
rownum = int(args.rownum)
if rownum is None:
    rownum = 10

#print(args.filename, args.rownum)

def get_data():
#"Driver={ODBC Driver 18 for SQL Server};Server=tcp:mydbserver1234.database.windows.net,1433;Database=data_warehouse;Uid=azureuser1;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';DATABASE='+database+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;') as conn:
        print('yes')
        with conn.cursor() as cursor:
            cursor.execute("SELECT TOP (10) * FROM [SalesLT].[Product]")
            #cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
            row = cursor.fetchone()
            while row:
                print (str(row[0]) + " " + str(row[1]))
                row = cursor.fetchone()

'''
create table stage.IndeedJobs
(
title varchar,
company_name varchar,
location_name varchar,
rating varchar,
contract_type varchar,
has_fast_apply varchar,
publish_date varchar);
'''
def run_azure_pipeline(filename):
    df = pd.read_csv(filename, delimiter=';', keep_default_na=False)
    print(df.head())
    df['contract'] = df['contract'].astype('str')
    df['rating'] = df['rating'].astype('str')
    df['fast_apply'] = df['fast_apply'].astype('str')
    df['publish_date'] = df['publish_date'].str.encode('ASCII', 'ignore')
    
    #tuples = [tuple(x) for x in df.head().values]
    #print(tuples)

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';DATABASE='+database+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10;') as conn:
        print('connected to Azure')
        with conn.cursor() as cursor:
            # instead of create or replace we use truncate and insert
            # because we cannot use "create as select" here!
            # test if select * can also be used

            return_val = cursor.execute("truncate table stage.IndeedJobs").rowcount
            print("truncate done: %d"%return_val)

            # Insert new data to stage
            counter = 1
            for row in df.itertuples():
                if counter > rownum:
                    break
                
                return_val = cursor.execute('''
                            INSERT INTO stage.IndeedJobs (title, company_name, location_name, rating, contract_type,
                                has_fast_apply, publish_date)'''
                            + "VALUES (?,?,?,?,?,?,?)"
                            ,
                            row.job_title, 
                            row.company,
                            row.location,
                            row.rating,
                            row.contract,
                            row.fast_apply,
                            row.publish_date
                            ).rowcount
                counter += 1
            conn.commit()
            print('Import done! counter:%d and result:%d'%(counter-1, return_val))

run_azure_pipeline(input_filename)