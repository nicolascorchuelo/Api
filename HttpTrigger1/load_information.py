import pandas as pd
import pyodbc

departments ='https://storageinformationpoc.blob.core.windows.net/files/departments.csv?si=admin&spr=https&sv=2021-06-08&sr=b&sig=IcRZoXnpRPnnUYIr3%2BBBqkMBBONIwOZ4VvAnZIK44cI%3D'
hired_employees = 'https://storageinformationpoc.blob.core.windows.net/files/hired_employees.csv?si=admin&spr=https&sv=2021-06-08&sr=b&sig=78Yqqe1WzHcwBgsO%2F%2Fr8ohYebkor%2FLksCkcbpCVVIiI%3D'
jobs = 'https://storageinformationpoc.blob.core.windows.net/files/jobs.csv?si=admin&spr=https&sv=2021-06-08&sr=b&sig=h5Mu8bkK13MlaCyBL8GhE%2Fgjyas8zqB36pfPSDHboTY%3D'

server = 'hubinfo.database.windows.net'
database = 'PoCLoadInformationcsv'
username = 'sysadmin'
password = 'B0g0t49315N*+' 
driver= '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

class load_information:

    def create_df():
        df_departments = pd.read_csv(departments,sep=',',names=["id","department"])
        df_hired_employees = pd.read_csv(hired_employees,sep=',',names=["id","name","datetime","department_id","job_id"])
        df_jobs = pd.read_csv(jobs,sep=',',names=["id","job"])

    def create_table(self):
        cursor = conn.cursor()
        cursor.execute('''IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hired_employees' and xtype='U')
        CREATE TABLE stg.hired_employees(
        	id int,
        	name varchar(1000),
        	datetime varchar(1000),
            department_id int,
            job_id int
        )
        GO

        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments' and xtype='U')
        CREATE TABLE stg.departments(
        	id int,
        	name varchar(1000),
        	datetime varchar(1000),
            department_id int,
            job_id int
        )
        GO

        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='jobs' and xtype='U')
        CREATE TABLE stg.jobs(
        	id int,
        	job varchar(1000)
        )

        ''')

        conn.commit()
        cursor.close()
        conn.close()

    def load_hired_employees():
        cursor = conn.cursor()
        cursor.fast_executemany = True
        for index, row in load_information.create_df().df_hired_employees.iterrows():
            cursor.execute("INSERT INTO stg.departments ([id],[name],[datetime],[department_id],[job_id]) \
                          values(?,?,?,?,?)", 
                          row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['value_stocks'], row['timestamp_datetime'])       
        conn.commit()
        cursor.close()
        conn.close()

    def load_departments():
        cursor = conn.cursor()
        cursor.fast_executemany = True
        for index, row in load_information.create_df().df_departments.iterrows():
            cursor.execute("INSERT INTO stg.hired_employees ([id],[department]) \
                          values(?,?)", 
                          row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['value_stocks'], row['timestamp_datetime'])       
        conn.commit()
        cursor.close()
        conn.close()

    def load_jobs(self):
        cursor = conn.cursor()
        cursor.fast_executemany = True
        for index, row in load_information.create_df().df_jobs.iterrows():
            cursor.execute("INSERT INTO stg.jobs ([id],[job]) \
                          values(?,?)", 
                          row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['value_stocks'], row['timestamp_datetime'])       
        conn.commit()
        cursor.close()
        conn.close()

load_information.create_df()