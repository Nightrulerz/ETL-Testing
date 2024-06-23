from flask import Flask, render_template, request, session
import logging
import mysql.connector as connector

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/get_search_term', methods=["GET"])
def get_search_term():
    if request.method == 'GET':
        searchterm = request.args['searchterm']
        mycursor = db_connection()
        query = f"SELECT * FROM products WHERE title LIKE '%{searchterm}%'"
        mycursor.execute(query)
        list_products = []
        for data in mycursor:
            list_products.append(data)
        mycursor.close()
        return list_products
    else:
        return render_template('index.html')


def db_connection():
    for count in range(3):
        try:
            mydb = connector.connect(host="localhost", user="xxxxxx",
                                     password="xxxxx", database="amazon_products")
            mycursor = mydb.cursor()
            return mycursor
        except Exception as error:
            logging.exception(
                f"Error occurred while connecting to Kafka, error is {error}")


if __name__ == "__main__":
    app.run(debug=True)
