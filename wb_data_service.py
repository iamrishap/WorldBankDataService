from datetime import datetime
from flask import Flask, request
from flask_restplus import Api, Resource
import heapq
import json
import re
import requests
import sqlite3

API_BASE_URL = 'http://api.worldbank.org/v2/'
app = Flask(__name__)
api = Api(app, version='1.0', default="REST API Endpoints", default_label=None,
          title='Data Service for World Bank Economic Indicators',
          description='A Flask-Restplus data service that allows a client to read some '
                      'publicly available economic indicator data for countries around '
                      'the world, and allow the consumers to access the data through a REST API')
DB_FILE = None
REQUEST_HEADERS = {"content-type": "application/json"}


def create_db(db_file):
    '''
    create a database connection to a SQLite database
    db_file: Your database's name.
    '''
    """ create a database connection to a SQLite database """
    global DB_FILE
    DB_FILE = db_file
    db_conn = sqlite3.connect(db_file)
    db_curr = db_conn.cursor()
    db_curr.execute("""CREATE TABLE IF NOT EXISTS COLLECTIONS (collection_id INTEGER PRIMARY KEY,
                   indicator TEXT, indicator_value TEXT, collection_name TEXT,
                   creation_time TEXT, entries TEXT)""")
    db_conn.commit()
    db_conn.close()


@api.doc(params={
                    "collection_id": "ID of the Collection. It specifies an Resource in the Collection",
                }
         )
@api.route('/<string:collection_name>')
class WorldBankCollectionsList(Resource):
    @staticmethod
    def get(collection_name):
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
        db_curr.execute(
            """SELECT creation_time, collection_id, collection_name, indicator 
            FROM COLLECTIONS WHERE collection_name={0}"""
                .format(
                "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        collection_items = []
        for row in rows:
            response_dict = dict()
            response_dict["location"] = "/" + collection_name + "/" + str(row[1])
            response_dict["collection_id"] = str(row[1])
            response_dict["creation_time"] = str(row[0])
            response_dict["indicator"] = str(row[3])
            collection_items.append(response_dict)
        if collection_items:
            return collection_items, 200
        else:
            return {}, 204

    @staticmethod
    def post(collection_name):
        indicator_id = request.form['data']

        # Check_collection entry already exists based on collection_name and indicator_id
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
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
            return {"message": "Bad request. \'indicator_id\' " + indicator_id +
                               " is not correct. Please check the POST data."}, 404

        indicator_data = requests.get(url=API_BASE_URL + 'countries/all/indicators/' + indicator_id +
                                          '?date=2013:2018&per_page=2000&format=json')
        if indicator_data.status_code != 200:
            return {"message": "Bad request. World Bank API not able to find the "
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

        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
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
        db_conn.close()
        return response_dict, 201


@api.doc(params={
                    "collection_name": "The name of the collection",
                    "collection_id": "ID of the Collection. It specifies an Resource in the Collection",
                }
         )
@api.route('/<string:collection_name>/<int:collection_id>')
class WorldBankCollection(Resource):
    @staticmethod
    def delete(collection_name, collection_id):
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
        db_curr.execute(
            """DELETE FROM COLLECTIONS WHERE collection_id={0} AND collection_name={1}"""
                .format(
                collection_id,
                "'" + collection_name + "'"
            )
        )
        rows_deleted = db_curr.rowcount
        db_conn.commit()
        db_conn.close()
        if rows_deleted > 0:
            return {"message": "Collection = {0} is removed from the database!".format(collection_id)}, 200
        else:
            return {"message": "Collection not found."}, 204

    @staticmethod
    def get(collection_name, collection_id):
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
        db_curr.execute(
            """SELECT entries FROM COLLECTIONS WHERE collection_id={0} AND collection_name={1}"""
                .format(
                collection_id,
                "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        if rows:
            json_data = json.loads(rows[0][0])
            json_data["collection_id"] = collection_id
            return json_data, 200
        else:
            return {"message": "No collection with this collection_id found."}, 204


@api.doc(params={
                    "collection_name": "The name of the collection",
                    "collection_id": "ID of the Collection. It specifies an Resource in the Collection",
                    "year": "Year of interest(for the specific economic indicator)",
                    "country": "Country of interest(for the specific economic indicator)"
                }
         )
@api.route('/<string:collection_name>/<int:collection_id>/<int:year>/<string:country>')
class WorldBankCollectionFiltered(Resource):

    @staticmethod
    def get(collection_name, collection_id, country, year):
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
        db_curr.execute(
            """SELECT entries, indicator FROM COLLECTIONS WHERE collection_id={0} AND collection_name={1}"""
                .format(
                collection_id,
                "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        if rows:
            json_data = json.loads(rows[0][0])
            indicator_id = json_data["indicator"]
            for indicator_entry in json_data["entries"]:
                if indicator_entry["country"].lower() == country.lower() and indicator_entry["date"] == str(year):
                    return {
                               "collection_id": collection_id,
                               "indicator": indicator_id,
                               "country": country,
                               "year": year,
                               "value": indicator_entry["value"]
                           }, 200
        return {"message": "No collection with this collection_name, "
                           "collection_id, country, year was found."}, 204


@api.doc(params={
                    "q": "top1..100 or bottom1..100 for getting the results",
                    "collection_name": "The name of the collection",
                    "collection_id": "ID of the Collection. It specifies an Resource in the Collection",
                    "year": "Year of interest(for the specific economic indicator)"
                }
         )
@api.route('/<string:collection_name>/<int:collection_id>/<int:year>')
class WorldBankCollectionArranged(Resource):
    @staticmethod
    def get(collection_name, collection_id, year):
        query = request.args.get('q', default=None, type=str)
        query_suitable = re.match(r"^(top|bottom)([1-9][0-9]?$|100)$", query.lower())
        if not query_suitable:
            return {"message": "API endpoint only support top1 to top100 and bottom1 to bottom100."
                               "Please input right query."}, 204
        top_bottom, desired_number = query_suitable.groups()[0], int(query_suitable.groups()[1])
        db_conn = sqlite3.connect(DB_FILE)
        db_curr = db_conn.cursor()
        db_curr.execute(
            """SELECT entries, indicator FROM COLLECTIONS WHERE collection_id={0} AND collection_name={1}"""
                .format(
                collection_id,
                "'" + collection_name + "'"
            )
        )
        rows = db_curr.fetchall()
        if rows:
            json_data = json.loads(rows[0][0])
            indicator_id = json_data["indicator"]
            indicator_value = json_data["indicator_value"]
            heap = [
                (
                    indicator_entry["value"] if top_bottom == "bottom" else -indicator_entry["value"],
                    indicator_entry
                )
                for indicator_entry in json_data["entries"]
                if indicator_entry["date"] == str(year) and indicator_entry["value"]
            ]
            desired_entries = heapq.nsmallest(desired_number, heap)
            return {
                       "collection_id": str(collection_id),
                       "indicator": indicator_id,
                       "indicator_value": indicator_value,
                       "entries": [entry[1] for entry in desired_entries]
                   }, 200
        return {"message": "No collection with this collection_name, "
                           "collection_id, country, year was found."}, 204


if __name__ == '__main__':
    create_db('rishap.db')
    app.run(debug=True)
