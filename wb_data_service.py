from datetime import datetime
from flask import Flask, request
from flask_restplus import Api, Resource
import json
import requests
import sqlite3
from sqlite3 import Error

API_BASE_URL = 'http://api.worldbank.org/v2/'
app = Flask(__name__)
api = Api(app, version='1.0', title='Data Service for World Bank Economic Indicators',
          description='A Flask-Restplus data service that allows a client to read some '
                      'publicly available economic indicator data for countries around '
                      'the world, and allow the consumers to access the data through a REST API')
db_conn = None
db_curr = None
REQUEST_HEADERS = {"content-type": "application/json"}


def create_db(db_file):
    '''
    create a database connection to a SQLite database
    db_file: Your database's name.
    '''
    """ create a database connection to a SQLite database """
    global db_conn
    global db_curr
    db_conn = sqlite3.connect(db_file)
    db_curr = db_conn.cursor()
    db_curr.execute("""CREATE TABLE COLLECTIONS (collection_id INTEGER PRIMARY KEY,
                   indicator TEXT, indicator_value TEXT, collection_name TEXT, 
                   creation_time TEXT, entries TEXT)""")
    db_conn.commit()


@api.route('/<string:collection_name>')
class WorldBankCollectionsList(Resource):
    @staticmethod
    def get(collection_name):
        global db_conn
        global db_curr
        db_curr.execute(
            """SELECT creation_time, collection_id, collection_name, indicator 
            FROM COLLECTIONS WHERE collection_name={0}"""
            .format(
             "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        db_conn.close()
        collection_items = []
        print(rows)
        for row in rows:
            response_dict = dict()
            response_dict["location"] = "/" + collection_name + "/" + str(row[1])
            response_dict["collection_id"] = str(row[1])
            response_dict["creation_time"] = str(row[0])
            response_dict["indicator"] = str(row[3])
            collection_items.append(response_dict)
        return collection_items, 200

    @staticmethod
    def post(collection_name):
        indicator_id = request.form['data']

        # Check_collection entry already exists based on collection_name and indicator_id
        global db_conn
        global db_curr

        db_curr.execute(
            """SELECT creation_time, collection_id, collection_name, indicator 
            FROM COLLECTIONS WHERE indicator={0} AND collection_name={1}"""
            .format(
                "'" + indicator_id + "'",
                "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        db_conn.close()
        if len(rows) > 0:
            response_dict = dict()
            response_dict["location"] = "/" + collection_name + "/" + str(rows[0][1])
            response_dict["collection_id"] = str(rows[0][1])
            response_dict["creation_time"] = str(rows[0][0])
            response_dict["indicator"] = indicator_id
            return response_dict, 200

        indicator_response = requests.get(url=API_BASE_URL + 'indicators/' + indicator_id + '?format=json')

        # In case, the indicator_id is not found, World Bank API returns a list of size 1 with a format like below:
        # [{"message":[{"id":"120","key":"Invalid value","value":"The provided parameter value is not valid"}]}]

        if indicator_response.status_code != 200 or len(indicator_response.json()) < 2:
            return {"success": "false", "message": "Bad request. \'indicator_id\' " + indicator_id +
                                                   " is not correct. Please check the POST data."}, 404

        indicator_data = requests.get(url=API_BASE_URL + 'countries/all/indicators/' + indicator_id +
                                      '?date=2013:2018&per_page=2000&format=json')
        if indicator_data.status_code != 200:
            return {"success": "false", "message": "Bad request. World Bank API not able to find the "
                                                   "details of indicator_id " + indicator_id}, 404

        entries_data = []
        indicator_value = None
        for indicator_data_item in indicator_data.json()[1]:
            if not indicator_value:
                indicator_value = indicator_data_item["indicator"]["value"]
            entries_data.append({
                "country": indicator_data_item["country"]["value"],
                "date": indicator_data_item["date"],
                "value": indicator_data_item["value"]
            })

        collection = dict()
        collection["creation_time"] = datetime.now().replace(microsecond=0).isoformat()
        collection["entries"] = entries_data
        collection["indicator"] = indicator_id
        collection["indicator_value"] = indicator_value

        db_curr.execute(
            """INSERT INTO COLLECTIONS (indicator, indicator_value, creation_time, collection_name, entries) 
            VALUES ({0}, {1}, {2}, {3}, {4})""".format(
                '"' + collection["indicator"] + '"',
                '"' + collection["indicator_value"] + '"',
                '"' + collection["creation_time"] + '"',
                '"' + collection_name + '"',
                '"' + json.dumps(collection).replace('"', '""') + '"'
            )
        )
        collection_id = db_curr.lastrowid
        response_dict = dict()
        response_dict["location"] = "/" + collection_name + "/" + str(collection_id)
        response_dict["collection_id"] = str(collection_id)
        response_dict["creation_time"] = collection["creation_time"]
        response_dict["indicator"] = indicator_id
        db_conn.commit()
        return response_dict, 201



@api.route('/<string:collection_name>/<int:collection_id')
class WorldBankCollectionsList(Resource):
    @staticmethod
    def delete(collection_name, collection_id):
        global db_conn
        global db_curr
        db_curr.execute(
            """DELETE FROM COLLECTIONS WHERE collection_id={0} AND collection_name={1}"""
            .format(
                "'" + collection_id + "'",
                "'" + collection_name + "'"
            )
        )
        db_conn.commit()
        if db_curr.rowcount > 0:
            return {"message": "Collection = {0} is removed from the database!".format(collection_id)}, 200
        else:
            return {}


if __name__ == '__main__':
    global db_conn
    global db_curr
    try:
        create_db('rishap.db')
        app.run(debug=True)
    except Error as e:
        print("Exception occurred. Details:" + str(e))
    finally:
        db_conn.close()

