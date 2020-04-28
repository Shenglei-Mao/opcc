# change port number if necessary
# total 15 items, with all value 0
# item 0 - 14
import mysql.connector

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword"
)

cursor = db.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS opcc0")
cursor.execute("CREATE DATABASE IF NOT EXISTS opcc1")
cursor.execute("CREATE DATABASE IF NOT EXISTS opcc2")
cursor.execute("CREATE DATABASE IF NOT EXISTS opcc_central")

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword",
  database="opcc0"
)

cursor = db.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS data (name INT, value INT, PRIMARY KEY (name))")


sql = "INSERT INTO data(name, value) VALUES (%s, %s)"
val = [(item, 0) for item in range(15)]
try:
    cursor.executemany(sql, val)
except mysql.connector.errors.IntegrityError:
    print("database already initiated")
else:
    db.commit()
    print(cursor.rowcount, "was inserted.")

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword",
  database="opcc1"
)

cursor = db.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS data (name INT, value INT, PRIMARY KEY (name))")


sql = "INSERT INTO data(name, value) VALUES (%s, %s)"
val = [(item, 0) for item in range(15)]
try:
    cursor.executemany(sql, val)
except mysql.connector.errors.IntegrityError:
    print("database already initiated")
else:
    db.commit()
    print(cursor.rowcount, "was inserted.")

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword",
  database="opcc2"
)

cursor = db.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS data (name INT, value INT, PRIMARY KEY (name))")


sql = "INSERT INTO data(name, value) VALUES (%s, %s)"
val = [(item, 0) for item in range(15)]
try:
    cursor.executemany(sql, val)
except mysql.connector.errors.IntegrityError:
    print("database already initiated")
else:
    db.commit()
    print(cursor.rowcount, "was inserted.")

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword",
  database="opcc_central"
)

cursor = db.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS data (name INT, value INT, PRIMARY KEY (name))")


sql = "INSERT INTO data(name, value) VALUES (%s, %s)"
val = [(item, 0) for item in range(15)]
try:
    cursor.executemany(sql, val)
except mysql.connector.errors.IntegrityError:
    print("database already initiated")
else:
    db.commit()
    print(cursor.rowcount, "was inserted.")
