import bisect
import json

import mysql.connector

import transaction_gen
a = [(1, 2), (8, 7)]
bisect.insort(a, (2, 1))
print(a)
print(a[1][1])

a = 1
if a == 0 and print("yte"):
    print("hello")
a = [1, 2, 3, 4]
print(a[0:])
(1, 2, 3)
print(a[3])

print(transaction_gen.random_short_transaction())

db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  passwd="nopassword",
  database="opcc_central"
)
cursor = db.cursor()
sql = "UPDATE DATA SET value = %s WHERE name = %s"
# val = (1, 2)
val = ((1, 12), (2, 12))
cursor.executemany(sql, val)
# cursor.execute(sql, val)
# cursor.execute(sql)