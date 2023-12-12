#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 15:23:37 2023

@author: megha
"""

import openai
import os
import boto3
from pymongo import MongoClient
import pandas as pd
import sys
from io import StringIO
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import streamlit as st
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
import json

logging.basicConfig(
    filename='app.log',  # Specify the log file name
    level=logging.INFO  # Set the logging level (INFO, WARNING, ERROR, etc.)
)


def create_drive_service():
    creds_dict = st.secrets["google_token"]["installed"]
    creds = Credentials.from_authorized_user_info(creds_dict)

    if not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

    service = build('drive', 'v3', credentials=creds)
    return service

def upload_file(file_path, folder_id, service):
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='text/plain')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')


openai.api_key  = st.secrets['OPENAI_API_KEY']

session = boto3.Session(
    aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY']
)

def get_completion_from_messages(messages, 
                                 model="gpt-3.5-turbo-16k", 
                                 temperature=0, 
                                 max_tokens=800):
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature, # this is the degree of randomness of the model's output
        max_tokens=max_tokens, # the maximum number of tokens the model can ouptut 
    )
    response = response.choices[0].message["content"]

    return response

secret = session.client(service_name='secretsmanager', region_name='us-east-1')

north_america = secret.get_secret_value(
    SecretId='arn:aws:secretsmanager:us-east-1:893141651859:secret:HSI-PROD-DB-READ-ONLY-USER-US-EAST-1-EVeOjL')

delmar = secret.get_secret_value(
    SecretId='arn:aws:secretsmanager:us-east-1:893141651859:secret:HSI-DELMAR-PROD-DB-READ-ONLY-USER-US-EAST-1-xtzlvS')

secret = session.client(service_name='secretsmanager', region_name='ap-southeast-1')

wira = secret.get_secret_value(
    SecretId='arn:aws:secretsmanager:ap-southeast-1:893141651859:secret:HSI-WIRAGO-PROD-DB-READ-ONLY-USER-AP-SOUTHEAST-1-NCgY6r')

south_east = secret.get_secret_value(
    SecretId='arn:aws:secretsmanager:ap-southeast-1:893141651859:secret:HSI-PROD-DB-READ-ONLY-USER-AP-SOUTHEAST-1-G8hGif')

secret = session.client(service_name='secretsmanager', region_name='eu-west-2')

europe = secret.get_secret_value(
    SecretId='arn:aws:secretsmanager:eu-west-2:893141651859:secret:HSI-PRD-DB-URL-EUWEST2-m5vyTZ')


delmar_uri = delmar['SecretString']
north_america_uri = north_america['SecretString']
wirago_uri = wira['SecretString']
south_east_uri = south_east['SecretString']
europe_uri = europe['SecretString']

north_america_client = MongoClient(north_america_uri)
delmar_client = MongoClient(delmar_uri)
wirago_client = MongoClient(wirago_uri)
south_east_client = MongoClient(south_east_uri)
europe_client = MongoClient(europe_uri)

delmar_database = delmar_client["delmar-order-management-prod"]
north_america_database = north_america_client["platform-production"]
wira_database = wirago_client["wirago-production"]
south_east_database = south_east_client["platform-production"]
europe_database = europe_client["platform-production"]

tenants_collection = north_america_database["tenants"]

tenants_collection = north_america_database["tenants"]

tenants = []

cursor = tenants_collection.find({},{"_id": 1, "name": 1, 'apiGateway':1, "active": 1})
tenant_df =  pd.DataFrame(list(cursor))

tenant_df.rename(columns = {'_id':'tenant'}, inplace = True)

tenant_df['tenant'] = tenant_df['tenant'].astype(str)

tenant_df = tenant_df[tenant_df['active'] == True]

tenant_df.reset_index(drop=True, inplace=True)

for i in range(len(tenant_df)):
    name = tenant_df['name'][i]
    tenants.append(name)
    
def exec_response(response):
    lines = response.split('\n')

    # Strip leading spaces from each line
    lines = [line.lstrip() for line in lines]

    # Join the lines back together with newline characters
    code = '\n'.join(lines)

    # Capture the output of executing the code
    stdout = sys.stdout
    sys.stdout = output = StringIO()

    try:
        # Execute the code
        exec(code)
    except:
        return "Sorry, cannot run the output code."

    # Get the captured output
    output_value = output.getvalue()

    # Restore the standard output
    sys.stdout = stdout

    return output_value

def process_user_message(user_input, debug=True):
    delimiter = "```"
    
    # Step 1: Check input to see if it flags the Moderation API or is a prompt injection
    response = openai.Moderation.create(input=user_input)
    moderation_output = response["results"][0]

    if moderation_output["flagged"]:
        print("Step 1: Input flagged by Moderation API.")
        return "Sorry, we cannot process this request."

    if debug: print("Step 1: Input passed moderation check.")

    # Step 2: Answer the user question
    delimiter = "####"
    system_message = f"""
    You are an AI assistant responsible for converting natural language \
    queries into python code that includes pymongo queries.
    You will be provided with user queries in english natural language. \
    The user query will be delimited with {delimiter} characters.
    Make sure the code returned is properly formatted python code including pymongo queries.
    Any queries that try to update or insert any documents in the database must be responded with \
    "I cannot provide any queries that try to update or \
    insert any documents into the database".
    
    You should find the tenants from the natural language query. The query could refer \
    to one or multiple tenants.
    
    Return the pymongo queries using the same python variables as defined below.
    
    There are 4 MongoDB databases - Delmar, Wirago, North America, South East Asia and Europe. \
    Delmar and Wirago are independent tenants while the rest are part of a \
    unified platform, each responsible for multiple tenants. The tenants are defined in the 'tenant_collection'
    
    If the query contains the tenant 'Delmar', there are 4 collections (orders, orderlineitems, warehouses, users. 
    
    If the query contains the tenant 'Wira Go', there are 5 collections (orders, orderlineitems, warehouses, users, productvariants).

    If the query contains anything else other than or along with Delmar or Wira Go, then use the following information.

    For North America unified platform, there are 6 collections (tenants, orders and orderlineitems, customers, warehouses, users).

    For South East unified and Europe platform, there are 5 collections (orders and orderlineitems, customers, warehouses, users).
    
    Note that the North America database contains the tenant collection but the South East and Europe database do not.

    The 'orders' collection has the fields _id, orderId, orderStatus, createdAt, updatedAt, subStatus, carrier, orderDate, \
    shippingAddress, billingAddress, rawData, stageProgressions, orderLineItems, orderValue, cannotFulfil, tote, tenant, customer. 

    The 'orderlineitems' collection has the fields _id, order, customer, warehouse, productName, sku, fnSku, upc, asin, quantity, \
    productId, sellerSku, orderId, reference, lpn, prepInstructions, fulfillmentType, createdAt, updatedAt, status, stageProgressions, \
    binLocation, tote, stockBin, availableQuantity, tenant.

    The 'warehouses' collection has the fields _id, code, name, location, createdAt, updatedAt, tenant, isDefault, splitOrdersEnabled, \
    typeOfWarehouse, active, address, storageTypes.
    
    The 'users' collection has the fields _id, name, username, password, role, hopstackModules, createdAt, updatedAt, permissions \
    pagePreferences, tenant, isDefault, customers, warehouses.
    
    The 'customers' collection has the fields _id, name, code, tenant, isDefault, createdAt, updatedAt, warehouses, currency, currentBillingProfile, active. 
    
    The 'productvariants' collection has the fields _id, name, sku, upc, productId, price, weight, weightUnit, quantity, customer \
    warehouse, createdAt, updatedAt, tenant, source, images, rawData, quantities.
    
    The 'name' field in the 'tenants' collection contains the name of the tenant and the pymongo query \
    might need extract the information for the tenants given in the input natural language query. The names of the tenants \
    are given in {tenants}. Use this to find the tenant name given in the query and match it to the tenants_collection.
    If a particular tenant in the unified platform (all tenants apart from Delmar and Wirago) is queried,
    then we must find the tenant id and apiGateway from the tenants collection after matching the name.
    If the apiGateway is 'https://api.prod.us-east-1.hopstack.io' then we query the north america database \
    with the extracted tenant id. If the apiGateway is 'https://api.prod.ap-southeast-1.hopstack.io' then we \
    query the south east database with extracted tenant id. If the apiGateway is 'https://api.prod.eu-west-2.hopstack.io' then we \
    query the europe database with extracted tenant id.
    
    Only return a python code containing pymongo query and nothing else. Use the variables given above in the query.
    Add a print() to the last line of the code that prints the answer.
    """
    
    few_shot_user_1 = """How many skus were sold in the last month for wirago?"""
    few_shot_assistant_1 = """ 
    # Current date and time
    today_datetime = datetime.now()

    # Calculate the date for the start and end of the last month
    last_month_end = today_datetime.replace(day=1) - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Convert datetime objects to ObjectId values for MongoDB queries
    start_oid = ObjectId.from_datetime(last_month_start)
    end_oid = ObjectId.from_datetime(last_month_end)

    # Aggregate the quantities of SKUs sold
    aggregation_result = wira_database.orderlineitems.aggregate([
        {
            "$match": {
                "_id": {"$gte": start_oid, "$lt": end_oid}
            }
        },
        {
            "$group": {
                "_id": None,  # Grouping without a specific field to get total sum
                "total_quantity": {"$sum": "$quantity"}  # Summing up the quantities
            }
        }
    ])

    # Extract the total quantity from the aggregation result
    total_quantity = next(aggregation_result, {}).get("total_quantity", 0)
    print(total_quantity)"""
    
    few_shot_user_2 = """Which is the sku that has sold the most for Delmar?"""
    few_shot_assistant_2 = """ 
    pipeline = [
            {"$group": {"_id": "$sku", "total_sold": {"$sum": "$quantity"}}},
            {"$sort": {"total_sold": -1}},
            {"$limit": 1}
        ]

    result = delmar_database.orderlineitems.aggregate(pipeline)

    most_sold_sku = next(result)["_id"]
    print(most_sold_sku)"""
    
    few_shot_user_3 = """Which is the tenant with the highest quantity of sales?"""
    few_shot_assistant_3 = """
    # Pipelines for aggregation
    pipeline_with_tenant = [
        {"$group": {
            "_id": "$tenant",
            "total_sales": {"$sum": "$quantity"}
        }}
    ]

    pipeline_without_tenant = [
        {"$group": {
            "_id": None,
            "total_sales": {"$sum": "$quantity"}
        }}
    ]

    # Aggregating sales for each database
    sales_na = list(north_america_database.orderlineitems.aggregate(pipeline_with_tenant))
    sales_se = list(south_east_database.orderlineitems.aggregate(pipeline_with_tenant))
    sales_eu = list(europe_database.orderlineitems.aggregate(pipeline_with_tenant))

    total_sales_delmar = list(delmar_database.orderlineitems.aggregate(pipeline_without_tenant))[0]['total_sales']
    total_sales_wira = list(wira_database.orderlineitems.aggregate(pipeline_without_tenant))[0]['total_sales']

    # Combining all sales data
    combined_sales = sales_na + sales_se + sales_eu
    combined_sales.append({"_id": "Delmar", "total_sales": total_sales_delmar})
    combined_sales.append({"_id": "Wira", "total_sales": total_sales_wira})

    # Finding the tenant with the highest sales
    highest_sales = max(combined_sales, key=lambda x: x['total_sales'])
    highest_sales_tenant_id = highest_sales['_id']
    highest_sales_number = highest_sales['total_sales']

    # Check if the highest sales tenant is neither Delmar nor Wira
    if highest_sales_tenant_id not in ["Delmar", "Wira"]:
        # Find the tenant's name using the tenant ID
        object_id = ObjectId(highest_sales_tenant_id)
        tenant_doc = tenants_collection.find_one({"_id": object_id})

        # Extracting the name
        tenant_name = tenant_doc['name']
        print(f"The tenant with the highest sales is {tenant_name} with total sales of {highest_sales_number}")
    else:
        # Handle the special cases where the highest sales are from Delmar or Wira
        print(f"The highest sales are from {highest_sales_tenant_id} with total sales of {highest_sales_number}")"""

    few_shot_user_4 = """How many orders were successfully completed last month by Wira?"""
    few_shot_assistant_4 = """
    # Get the current date and time
    today_datetime = datetime.now()

    # Calculate the date for the start and end of the last month
    last_month_end = today_datetime.replace(day=1) - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Convert the datetime objects to ObjectId values for MongoDB queries
    start_oid = ObjectId.from_datetime(last_month_start)
    end_oid = ObjectId.from_datetime(last_month_end)

    # Query the wira_order_collection for the number of successfully completed orders
    completed_orders = wira_database.orders.count_documents({
        "orderStatus": "COMPLETED",
        "_id": {
            "$gte": start_oid,
            "$lt": end_oid
        }
    })
    print(completed_orders)

    """
    few_shot_user_5 = """How many orders were shipped to 'North Carolina' in the past six months by Delmar?"""
    few_shot_assistant_5 = """
    # Get the current date and time
    today_datetime = datetime.now()

    # Calculate the date for six months ago
    six_months_ago = today_datetime - timedelta(days=180)

    # Convert the datetime objects to ObjectId values for MongoDB queries
    start_oid = ObjectId.from_datetime(six_months_ago)
    end_oid = ObjectId.from_datetime(today_datetime)

    # Query the delmar_order_collection for the number of orders shipped to 'North Carolina'
    shipped_orders = delmar_database.orders.count_documents({
        "shippingAddress.state": "North Carolina",
        "_id": {
            "$gte": start_oid,
            "$lte": end_oid
        }
    })
    print(shipped_orders)
    """
    
    few_shot_user_6 = """How many orders were successfully completed in the past year by prime zero prep?"""
    few_shot_assistant_6 = """
    # Find the tenant document
    tenant_doc = tenants_collection.find_one({"name": "Prime Zero Prep"})

    # Get tenant id and apiGateway
    tenant_id = tenant_doc['_id']
    api_gateway = tenant_doc['apiGateway']

    # Choose the appropriate collection
    if api_gateway == "https://api.prod.us-east-1.hopstack.io":
        order_collection = north_america_database['orders']
    elif api_gateway == 'https://api.prod.ap-southeast-1.hopstack.io':
        order_collection = south_east_database['orders']
    elif api_gateway == 'https://api.prod.eu-west-2.hopstack.io':
        order_collection = europe_database['orders']

    # Fetch a sample document from the order collection
    sample_doc = order_collection.find_one({"tenant": str(tenant_id)})

    # Set the start and end dates for the last month
    today = datetime.now()
    last_six_months_start = today - timedelta(days=365)

    # Convert the datetime objects to ObjectId values for MongoDB queries
    start_oid = ObjectId.from_datetime(last_six_months_start)
    end_oid = ObjectId.from_datetime(today)

    # Query the database for the number of successfully completed orders
    completed_orders = order_collection.count_documents({
        "tenant": str(tenant_id),
        "orderStatus": "COMPLETED",
        "_id": {
            "$gte": start_oid,
            "$lte": end_oid
        }
    })

    print(completed_orders)

    """
    
    few_shot_user_7 = """How many SKUs have less than 3 orders during their lifetime for Nawel?"""
    few_shot_assistant_7 = """
    tenant_doc = tenants_collection.find_one({"name": "Nawel"})

    # Get tenant id and apiGateway
    tenant_id = tenant_doc['_id']
    api_gateway = tenant_doc['apiGateway']

    # Choose the appropriate collection
    if api_gateway == "https://api.prod.us-east-1.hopstack.io":
        order_collection = north_america_database.orders
        orderlineitems_collection = north_america_database.orderlineitems
    elif api_gateway == 'https://api.prod.ap-southeast-1.hopstack.io':
        order_collection = south_east_database.orders
        orderlineitems_collection = south_east_database.orderlineitems
    elif api_gateway == 'https://api.prod.eu-west-2.hopstack.io':
        order_collection = europe_database.orders
        orderlineitems_collection = europe_database.orderlineitems


    # Fetch a sample document from the order collection
    sample_doc = order_collection.find_one({"tenant": str(tenant_id)})

    # Query the database for the SKUs with less than 3 orders
    skus_less_than_3_orders = orderlineitems_collection.aggregate([
        {"$match": {"tenant": str(tenant_id)}},
        {"$group": {"_id": "$sku", "total_orders": {"$sum": 1}}},
        {"$match": {"total_orders": {"$lt": 3}}}
    ])

    sku_count = 0
    for sku in skus_less_than_3_orders:
        sku_count += 1

    print(sku_count)

    """
    few_shot_user_8 = """How many tenants are there? List all the tenants"""
    few_shot_assistant_8 = """ 
    tenants = []

    cursor = tenants_collection.find({},{"_id": 1, "name": 1, 'apiGateway':1, "active": 1})
    tenant_df =  pd.DataFrame(list(cursor))

    tenant_df.rename(columns = {'_id':'tenant'}, inplace = True)

    tenant_df['tenant'] = tenant_df['tenant'].astype(str)

    tenant_df = tenant_df[tenant_df['active'] == True]

    tenant_df.reset_index(drop=True, inplace=True)

    for i in range(len(tenant_df)):
        name = tenant_df['name'][i]
        tenants.append(name)

    tenants_str = ', '.join(tenants)

    # Printing the formatted string
    print(f"There are {len(tenants)} tenants, which are {tenants_str}")"""

    few_shot_user_9 = """How many orders were completed yesterday?"""
    few_shot_assistant_9 = """ 
    # Calculate yesterday's date
    today = datetime.now()
    yesterday_start = today - timedelta(days=1)
    yesterday_start = yesterday_start.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday_start + timedelta(days=1) - timedelta(microseconds=1)

    # Convert the datetime objects to ObjectId values for MongoDB queries
    start_oid = ObjectId.from_datetime(yesterday_start)
    end_oid = ObjectId.from_datetime(yesterday_end)

    north_america_order_collection = north_america_database['orders']
    south_east_order_collection = south_east_database['orders']
    europe_order_collection = europe_database['orders']
    delmar_order_collection = delmar_database['orders']
    wira_order_collection = wira_database['orders']

    # Define the collections for each tenant or database
    collections = {
        "North America": north_america_order_collection,
        "South East": south_east_order_collection,
        "Europe": europe_order_collection,
        "Delmar": delmar_order_collection,
        "Wira": wira_order_collection
    }

    total_completed_orders = 0

    # Iterate through each collection and count the completed orders
    for name, collection in collections.items():
        completed_orders = collection.count_documents({
            "orderStatus": "COMPLETED",
            "_id": {
                "$gte": start_oid,
                "$lte": end_oid
            }
        })
        total_completed_orders += completed_orders

    print(f"Total number of orders completed yesterday across all tenants: {total_completed_orders}")
    """

    messages =  [  
    {'role':'system', 'content': system_message},    
    {'role':'user', 'content': f"{delimiter}{few_shot_user_1}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_1 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_2}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_2 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_3}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_3 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_4}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_4 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_5}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_5 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_6}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_6 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_7}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_7 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_8}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_8 },
    {'role':'user', 'content': f"{delimiter}{few_shot_user_9}{delimiter}"},  
    {'role':'assistant', 'content': few_shot_assistant_9},
    {'role':'user', 'content': f"{delimiter}{user_input}{delimiter}"},  
    ] 

    final_response = get_completion_from_messages(messages)
    
    with open('qa.txt', "w") as file:
        file.write("Response: " + final_response)
    if debug:
        print("Step 3: Generated response to user question.")

    # Step 3: Put the answer through the Moderation API
    response = openai.Moderation.create(input=final_response)
    moderation_output = response["results"][0]

    if moderation_output["flagged"]:
        if debug: print("Step 5: Response flagged by Moderation API.")
        return "Sorry, we cannot provide this information."

    if debug: print("Step 4: Response passed moderation check.")
    
    # Step 4: Ask the model if the response answers the initial user query well
    user_message = f"""
    Agent response: {delimiter}{final_response}{delimiter}

    If the response is a properly formatted python code that is not trying to \
    insert or update any documents in the database, respond with Y. If it is not \
    python code or is trying to insert or update documents, respond with N
    """
    messages = [
        {'role': 'system', 'content': system_message},
        {'role': 'user', 'content': user_message}
    ]
    
    evaluation_response = get_completion_from_messages(messages)
    if debug:
        print("Step 5: Model evaluated if the response is a python code.")
        
    #Step 7: If yes, use this answer; if not, say that you cannot provide the information
    if "Y" in evaluation_response:  # Using "in" instead of "==" to be safer for model output variation (e.g., "Y." or "Yes")
        if debug: print("Step 6: Model approved the response.")
    else:
        if debug: print("Step 6: Model disapproved the response.")
        neg_str = "I'm unable to provide the information you're looking for."
        return neg_str

    if debug: print(final_response)
    
    #Step 8: Format and run the code and return output
    output_value = exec_response(final_response)
    if debug: print("Step 7: Executed generated python code.")
    return output_value



question_input = st.text_input("Question:")
with open('qa.txt', "w") as file:
    file.write("Question: " + question_input)

folder_id = '1aj2VjXTW_Tv2eti38HgjIUkc7PQQmQSw'  
qa_file_path = 'qa.txt'  

service = create_drive_service()

if question_input:
    response = process_user_message(question_input)
else:
    response = ""
    
file_id = upload_file(qa_file_path, folder_id, service)

st.text_area("Answer:", response, height=300)
