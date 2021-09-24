from mysql.connector import (connection)

#establishing the connection
conn = connection.MySQLConnection(user='root',
                                  password='Abrilow@13', 
                                  host='127.0.0.1'
                                )
cursor = conn.cursor()
#Droping database MYDATABASE if already exists.
cursor.execute("DROP database IF EXISTS MyDatabase")

#Preparing query to create a database
sql = "CREATE database MYDATABASE";

#Creating a database
cursor.execute(sql)

#Retrieving the list of databases
# print("List of databases: ")
# cursor.execute("SHOW DATABASES")
# print(cursor.fetchall())

#Closing the connection
conn.close()