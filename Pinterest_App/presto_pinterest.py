import prestodb
import pandas as pd

connection = prestodb.dbapi.connect(
    host='localhost',
    catalog='cassandra',
    user='Martin',
    port='8080',
    schema='data'
)

cur =connection.cursor()
cur.execute("SELECT follower_count AS travel_flw_count FROM pinterest_data WHERE category = 'travel'")
rows = cur.fetchall()

pinterest_db = pd.DataFrame(rows, columns=['travel_follower_count'])
print(pinterest_db)