# importing the required modules
from json import loads
from kafka import KafkaConsumer
import mysql.connector as connector
from csv import DictReader
import logging


def connecting_consumer():
    for count in range(3):
        try:
            my_consumer = KafkaConsumer(
                'testnum',
                bootstrap_servers=['localhost : 9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='my-group',
                value_deserializer=lambda x: loads(x.decode('utf-8'))
            )
            return my_consumer
        except Exception as error:
            logging.exception(
                f"Error occurred while connecting to Kafka, error is {error}")

def db_connection():
    for count in range(3):
        try:
            mydb = connector.connect(host="localhost", user="ganesh",
                                    password="password", database="amazon_products")
            mycursor = mydb.cursor()
            return mycursor, mydb
        except Exception as error:
            logging.exception(
                f"Error occurred while connecting to Kafka, error is {error}")

my_consumer = connecting_consumer()
mycursor, mydb = db_connection()
# mycursor.execute("CREATE TABLE products (title VARCHAR(250), price VARCHAR(50), description VARCHAR(5000), brand VARCHAR(20), asin VARCHAR(12), rating FLOAT(4, 2), review INTEGER(10), breadcrumb VARCHAR(200), category VARCHAR(50), product_url VARCHAR(200))")
# mycursor.execute("ALTER TABLE products MODIFY COLUMN description VARCHAR(10000)")
# mycursor.execute("DELETE FROM products")

count = 1
for message in my_consumer:
    row = message.value
    print(count)
    try:
        values = (row.get("title"), row.get('price'), row.get('description'), row.get('brand'), row.get('asin'), row.get(
            'rating', None), row.get('review', None), row.get('breadcrumb'), row.get('category'), row.get('product_url'))
        mycursor.execute(
            "INSERT INTO products (title, price, description, brand, asin, rating, review, breadcrumb, category, product_url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)
    except Exception as error:
        print(error)
    count += 1
    mydb.commit()
mycursor.close()

# mycursor.execute("SELECT * FROM products WHERE title LIKE '%shoe%'")
