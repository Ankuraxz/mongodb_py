import os
import certifi
import motor.motor_asyncio
import json
import time
import pymongo
import logging
import asyncio
from bson import ObjectId #install bson with pymongo - pip install pymongo
#LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Setting up the connection
# username = "admin"
# password = admin
uri = "mongodb+srv://admin:admin@cluster0.ihzdk52.mongodb.net/?retryWrites=true&w=majority"
try:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(uri,tlsCAFile=certifi.where())
    print("Connected successfully!!!")
except Exception as e:
    print(e)

#Object ID Factory
class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


#Creating a database
db = mongo_client["test"]

#Creating a collection
collection = db["test_collection"]

#inserting a document
async def insert_document(document:dict):
    try:
        result = await collection.insert_one(document)
        return result
    except Exception as e:
        logger.error(e)
        return None

#updating a document
async def update_document(document:dict):
    try:
        result = await collection.update_one({"_id":document["_id"]},{"$set":document},upsert=True)
        return result
    except Exception as e:
        logger.error(e)
        return None

# Get a document
async def get_document(document_id:str):
    try:
        result = await collection.find_one({"_id":document_id})
        return result
    except Exception as e:
        logger.error(e)
        return None

# Insert many
async def insert_many_documents(documents:list):
    try:
        result = await collection.insert_many(documents)
        return result
    except Exception as e:
        logger.error(e)
        return None

#Create Index
async def create_index(collection):
    try:
        result = await collection.create_index([('text', pymongo.TEXT)])
        return result
    except Exception as e:
        logger.error(e)
        return None

# Find with Query
async def find_with_query(collection,query):
    try:
        cursor = collection.find(query)
        docs=[]
        async for document in cursor:
            docs.append(document)
        return docs
    except Exception as e:
        logger.error(e)
        return None

#Delete many
async def delete_many_documents(collection):
    try:
        result = await collection.delete_many({})
        return result
    except Exception as e:
        logger.error(e)
        return None

#####################################
async def main():

    #create a document
    document = {"_id":str(ObjectId()),
                "name":"test",
                "email":"tester@gmail.com",
                "password":"test123",
                "phone":"1234567890",
                "address":"test address",
                "city":"test city",
                "state":"test state",
                "country":"test country",
                "pincode":"123456",
                "created_at":time.time(),
                }
# Working with a Single Document
    print("Inserting a document")
    #running with asyncio
    result = await insert_document(document)
    # Print the ID of the inserted document
    print(f'Insertion Done with ID: {result.inserted_id}')
    idx = result.inserted_id
    print("Finding a document")
    result = await get_document(idx)
    print(result)

    print("Updating a document")
    document["name"] = "test1"
    result = await update_document(document)

    print("Finding a document")
    result = await get_document(idx)
    print(result)

    print("Deleting a document")
    result = await collection.delete_one({"_id":idx})
    print(result)

# Inserting multiple documents with $Text
    print("Inserting multiple documents")
    # Insert documents with the `$text` field
    doc1 = {'_id': str(ObjectId()), 'text': 'This is the first document.', 'age': 25}
    doc2 = {'_id': str(ObjectId()), 'text': 'This is the second document.', 'age': 30}
    doc3 = {'_id': str(ObjectId()), 'text': 'This is the third document.', 'age': 60}

    # Insert the documents
    result = await insert_many_documents([doc1, doc2, doc3])
    print(f'Insertion Done with ID: {result.inserted_ids}')


    # Create the text index
    result = await create_index(collection)
    print(f'Index created: {result}')

    # Search for a document using text
    query = {'$text': {'$search': 'second'}}
    result = await find_with_query(collection,query)
    for doc in result:
        print(doc)

    print("Filtering")
    # filter by age
    query1 = {'age': {'$gt': 25}} #greater than 25
    query2 = {'age': {'$lt': 60}} #less than 60
    query3 = {'age': {'$gte': 25}} #greater than or equal to 25
    query4 = {'age': {'$lte': 60}} #less than or equal to 60
    query5 = {'age': {'$ne': 30}} #not equal to 30
    result = await find_with_query(collection,query1)
    for doc in result:
        print(doc)
    result = await find_with_query(collection,query2)
    for doc in result:
        print(doc)
    result = await find_with_query(collection,query3)
    for doc in result:
        print(doc)
    result = await find_with_query(collection,query4)
    for doc in result:
        print(doc)
    result = await find_with_query(collection,query5)
    for doc in result:
        print(doc)


    print("Combining 2 queries")
    #Combining 2 queries
    query = {'$and': [{'age': {'$gt': 25}}, {'age': {'$lt': 60}}]}
    result = await find_with_query(collection,query)
    for doc in result:
        print(doc)


    # # Delete the documents
    # result = await delete_many_documents(collection)



if __name__ == "__main__":
    asyncio.run(main())







