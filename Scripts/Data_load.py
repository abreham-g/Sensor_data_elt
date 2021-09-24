from mysql.connector import (connection)
from sqlalchemy import create_engine, types
import mysql.connector as mysql
import pandas as pd


my_data = "MyDatabase"

class data_warehouse:
    def __init__(self, Host:str, user:str,my_password: str) -> None:
        self.conn = self.conn_database(Host, user, my_password)
        self.cursor = self.conn_database(Host, user, my_password)
        pass
    def conn_database(self, Host: str, user: str, my_password: str, dbName: str = "MyDatabase"):
        
        try:
            conn = mysql.connect(host=Host, 
                                user=user, 
                                password=my_password,
                                #auth_plugin='mysql_native_password',
                                database=dbName, 
                                buffered=True)
            return conn, conn.cursor()

        except Exception as ex:
            conn = mysql.connect(host=Host, 
                                 user=user,
                                 #auth_plugin='mysql_native_password', 
                                 password=my_password, 
                                 buffered=True)
            cursor = conn.cursor() 
            sql = f"CREATE DATABASE {my_data}"
            cursor.execute(sql)

            return conn, cursor
    
    def load_Data(self, dbName: str,  host: str, user: str, password: str, tableName: str, csv_file: str):
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{dbName}')
        conn = mysql.connect(host=host, 
                                user=user, 
                                password=password,
                                #auth_plugin='mysql_native_password',
                                database=dbName, 
                                buffered=True)

        try:
            dataframe = pd.read_csv(csv_file, sep=',', quotechar='\'', encoding='utf8')
            dataframe.to_sql(tableName, con=engine, index=False, if_exists='append')
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
if __name__ == "__main__":
    TABLE_NAME = "sample"
    a = data_warehouse(Host = "localhost", user = "root", my_password = "Abrilow@13")
    a.load_Data(my_data , "localhost", "root", "Abrilow@13", TABLE_NAME, "./I80_sample.txt" )