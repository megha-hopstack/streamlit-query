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
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import logging
import json
import tempfile

logging.basicConfig(
    filename='app.log',  # Specify the log file name
    level=logging.INFO  # Set the logging level (INFO, WARNING, ERROR, etc.)
)

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_google_drive():
    creds = None
    token_info = st.secrets["google_token"]["installed"]
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
       json.dump(token_info, temp_file)
       temp_file_path = temp_file.name
   
   # Use the temporary file in the credentials method
    creds = Credentials.from_authorized_user_file(temp_file_path, SCOPES)

   # Optional: delete the temporary file if you want
    os.remove(temp_file_path)

def upload_file_to_drive(filename, folder_id, drive_service):
    file_metadata = {
        'name': filename,
        'parents': [folder_id]  # Specify the folder ID
    }
    media = MediaFileUpload(filename, mimetype='text/plain')
    file = drive_service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()

    

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
    response = response.strip('"Output:\n```\n')
    response = response.split("\n```\nExplanation:\n")[0].strip()
    response = response.split("\n```\n\nExplanation:\n")[0].strip()

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

wira_order_collection = wira_database["orders"]
wira_orderlineitems_collection = wira_database["orderlineitems"]
wira_customers_collection = wira_database["customers"]
wira_warehouses_collection = wira_database["warehouses"]
wira_products_collection = wira_database["productvariants"]

delmar_order_collection = delmar_database["orders"]
delmar_orderlineitems_collection = delmar_database["orderlineitems"]
delmar_warehouses_collection = delmar_database["warehouses"]
delmar_users_collection = delmar_database["users"]

north_america_order_collection = north_america_database["orders"]
north_america_orderlineitems_collection = north_america_database["orderlineitems"]
north_america_customers_collection = north_america_database["customers"]
north_america_warehouses_collection = north_america_database["warehouses"]
north_america_users_collection = north_america_database["users"]
north_america_products_collection = north_america_database["productvariants"]

south_east_order_collection = south_east_database["orders"]
south_east_orderlineitems_collection = south_east_database["orderlineitems"]
south_east_customers_collection = south_east_database["customers"]
south_east_warehouses_collection = south_east_database["warehouses"]
south_east_users_collection = south_east_database["users"]
south_east_products_collection = south_east_database["productvariants"]

europe_order_collection = europe_database["orders"]
europe_orderlineitems_collection = europe_database["orderlineitems"]
europe_customers_collection = europe_database["customers"]
europe_warehouses_collection = europe_database["warehouses"]
europe_users_collection = europe_database["users"]
europe_products_collection = europe_database["productvariants"]

cursor = tenants_collection.find({},{"_id": 1, "name": 1, 'apiGateway':1, "active": 1})
tenant_df =  pd.DataFrame(list(cursor))

tenant_df.rename(columns = {'_id':'tenant'}, inplace = True)

tenant_df['tenant'] = tenant_df['tenant'].astype(str)

tenant_df = tenant_df[tenant_df['active'] == True]
tenant_df = tenant_df[tenant_df['name'] != 'Hopstack Inc']
tenant_df = tenant_df[tenant_df['name'] != 'Hopstack']
tenant_df = tenant_df[tenant_df['name'] != 'Delmar']
tenant_df = tenant_df[tenant_df['name'] != 'Ops Test Inc']
tenant_df = tenant_df[tenant_df['name'] != 'Starter']
tenant_df = tenant_df[tenant_df['name'] != 'Feature Test']
tenant_df = tenant_df[tenant_df['name'] != 'IFD']
tenant_df = tenant_df[tenant_df['name'] != 'TYM Tractors']
tenant_df = tenant_df[tenant_df['name'] != 'Hooli Inc']
tenant_df = tenant_df[tenant_df['name'] != 'KGW Logistics']
tenant_df = tenant_df[tenant_df['name'] != 'Sometime Malaysia']
tenant_df = tenant_df[tenant_df['name'] != 'Hopstack Dev']
tenant_df = tenant_df[tenant_df['name'] != 'Hopstack-Sneha']

tenant_df.reset_index(drop=True, inplace=True)

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
        logging.debug("Step 1: Input flagged by Moderation API.")
        return "Sorry, we cannot process this request."

    logging.debug("Step 1: Input passed moderation check.")

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
    
    If the query contains the tenant 'Delmar', there are 4 collections (orders, orderlineitems, warehouses, users) for which the python variables \
    defined are delmar_order_collection, delmar_orderlineitems_collection, delmar_warehouses_collection, delmar_users_collection. 
    
    If the query contains the tenant 'Wira Go', there are 5 collections (orders, orderlineitems, warehouses, users, productvariants)
    for which the python variables defined are wira_order_collection, wira_orderlineitems_collection, wira_customers_collection, wira_warehouses_collection, wira_products_collection.

    If the query contains anything else other than or along with Delmar or Wira Go, then use the following information.

    For North America unified platform, there are 6 collections (tenants, orders and orderlineitems, customers, warehouses, users) \
    for which the python variables defined are

    For South East unified and Europe platform, there are 5 collections (orders and orderlineitems, customers, warehouses, users) \
    for which the python variables defined are south_east_order_collection, south_east_orderlineitems_collection, south_east_customers_collection, south_east_warehouses_collection, \
    south_east_users_collection, south_east_products_collection, europe_order_collection, europe_orderlineitems_collection, europe_customers_collection, europe_warehouses_collection, \
    europe_users_collection, europe_products_collection
    
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
    must extract the information for only the tenants given in the input natural language query. 
    If a particular tenant in the unified platform (all tenants apart from Delmar and Wirago) is queried,
    then we must find the tenant id and apiGateway from the tenants collection after matching the name.
    If the apiGateway is 'https://api.prod.us-east-1.hopstack.io' then we query the north america database \
    with the extracted tenant id. If the apiGateway is 'https://api.prod.ap-southeast-1.hopstack.io' then we \
    query the south east database with extracted tenant id. If the apiGateway is 'https://api.prod.eu-west-2.hopstack.io' then we \
    query the europe database with the extracted tenant id. 
    
    Extract the date fields from the given jsons and ensure that any date related python code is \
    compatible with the date formats used in the database.
    
    Only return a python code containing pymongo query and nothing else. Use the variables given above in the query.
    Add a print() to the last line of the code that prints the answer.
    """
    
    few_shot_user_1 = """How many skus were sold in the last month for wirago?"""
    few_shot_assistant_1 = """ 
    from datetime import datetime, timedelta
    import calendar

    # Set the start and end dates for the last month
    today = datetime.strptime("2023-06-14 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_month_end = today.replace(day=1) - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Convert the dates to the required format
    start_date = int(last_month_start.timestamp() * 1000)
    end_date = int(last_month_end.timestamp() * 1000)

    # Query the database for the number of SKUs sold
    sold_skus = wira_database.orderlineitems.count_documents({
        "createdAt": {
            "$gte": start_date,
            "$lte": end_date
        }
    })
    print(sold_skus)"""
    
    few_shot_user_2 = """Which is the sku that has sold the most for Delmar?"""
    few_shot_assistant_2 = """ 
    pipeline = [
        {"$group": {"_id": "$sku", "total_sold": {"$sum": "$quantity"}}},
        {"$sort": {"total_sold": -1}},
        {"$limit": 1}
    ]

    result = delmar_orderlineitems_collection.aggregate(pipeline)

    most_sold_sku = next(result)["_id"]
    print(most_sold_sku)"""
    
    few_shot_user_3 = """Which is the tenant from the unified \
    platforms with the highest quantity of sales?"""
    few_shot_assistant_3 = """
    pipeline = [
        {"$group": {
            "_id": "$tenant",
            "total_sales": {"$sum": "$quantity"}
        }},
        {"$sort": {"total_sales": -1}},
        {"$limit": 1}
    ]

    result_na = list(north_america_database.orderlineitems.aggregate(pipeline))
    result_se = list(south_east_database.orderlineitems.aggregate(pipeline))

    max_sales_na = result_na[0]['total_sales'] if result_na else 0
    max_sales_se = result_se[0]['total_sales'] if result_se else 0

    print(max_sales_na)
    print(max_sales_se)

    if max_sales_na > max_sales_se:
        max_sales_tenant_id = result_na[0]['_id']
        max_sales_db = north_america_database
    else:
        max_sales_tenant_id = result_se[0]['_id']
        max_sales_db = south_east_database

    max_sales_tenant = max_sales_db.tenants.find_one({"_id": ObjectId(max_sales_tenant_id)})
    print(max_sales_tenant)"""
    
    few_shot_user_4 = """How many orders were successfully completed last month by Wira?"""
    few_shot_assistant_4 = """
    from datetime import datetime, timedelta

    # Set the start and end dates for the last month
    today = datetime.strptime("2023-06-14 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_month_end = today.replace(day=1) - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # Convert the dates to the required format
    start_date = last_month_start.strftime("%Y-%m-%d")
    end_date = last_month_end.strftime("%Y-%m-%d")

    # Query the database for the number of successfully completed orders
    completed_orders = wira_order_collection.count_documents({
        "orderStatus": "COMPLETED",
        "orderDate": {
            "$gte": start_date,
            "$lte": end_date
        }
    })
    print(completed_orders)
    """
    few_shot_user_5 = """How many orders were shipped to 'North Carolina' in the past six months by Delmar?"""
    few_shot_assistant_5 = """
    from datetime import datetime, timedelta

    # Set the start and end dates for the past six months
    today = datetime.strptime("2023-06-14 00:00:00", "%Y-%m-%d %H:%M:%S")
    six_months_ago = today - timedelta(days=180)

    # Query the database for the number of orders shipped to 'North Carolina'
    shipped_orders = delmar_order_collection.count_documents({
        "shippingAddress.state": "North Carolina",
        "orderDate": {
            "$gte": six_months_ago,
            "$lte": today
        }
    })
    print(shipped_orders)
    """
    
    few_shot_user_6 = """How many orders were successfully completed in the last 6 months by prime zero prep?"""
    few_shot_assistant_6 = """
    from datetime import datetime, timedelta

    # Find the tenant document
    tenant_doc = tenants_collection.find_one({"name": "Prime Zero Prep"})
    if tenant_doc is None:
        print("Tenant not found.")
        exit(1)

    # Get tenant id and apiGateway
    tenant_id = tenant_doc['_id']
    api_gateway = tenant_doc['apiGateway']

    # Choose the appropriate collection
    if api_gateway == "https://api.prod.us-east-1.hopstack.io":
        order_collection = north_america_order_collection
    elif api_gateway == 'https://api.prod.ap-southeast-1.hopstack.io':
        order_collection = south_east_order_collection
    else:
        print("Unknown apiGateway.")
        exit(1)

    # Fetch a sample document from the order collection
    sample_doc = order_collection.find_one({"tenant": str(tenant_id)})
    if sample_doc is None:
        print("No orders found for tenant.")
        exit(1)

    # Set the start and end dates for the last month
    today = datetime.strptime("2023-06-14 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_month_start = today - timedelta(days=180)
    last_month_end = today

    # Check if 'orderDate' is a string, a datetime object, or a Unix timestamp
    if isinstance(sample_doc['orderDate'], datetime):
        # If 'orderDate' is a datetime object, just use the datetime objects for comparison
        start_date = last_month_start
        end_date = last_month_end
    elif isinstance(sample_doc['orderDate'], str):
        # If 'orderDate' is a string, convert the datetime objects to strings
        start_date = last_month_start.strftime("%Y-%m-%d")
        end_date = last_month_end.strftime("%Y-%m-%d")
    elif isinstance(sample_doc['orderDate'], (int, float)):
        # If 'orderDate' is a Unix timestamp (assumed to be an integer or a float), convert the datetime objects to Unix timestamps
        start_date = int(last_month_start.timestamp() * 1000)  # Multiplied by 1000 to convert from seconds to milliseconds
        end_date = int(last_month_end.timestamp() * 1000)
    else:
        print("Unknown date format in order collection.")
        exit(1)

    print(start_date, end_date)

    # Query the database for the number of successfully completed orders
    completed_orders = order_collection.count_documents({
        "tenant": str(tenant_id),
        "orderStatus": "COMPLETED",
        "orderDate": {
            "$gte": start_date,
            "$lte": end_date
        }
    })

    print(completed_orders)

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
    {'role':'user', 'content': f"{delimiter}{user_input}{delimiter}"},  
    ] 

    final_response = get_completion_from_messages(messages)
    logging.debug("Step 2: Generated response to user question.")
    logging.info(f"Response: {final_response}")

    # Step 3: Put the answer through the Moderation API
    response = openai.Moderation.create(input=final_response)
    moderation_output = response["results"][0]

    if moderation_output["flagged"]:
        logging.debug("Response flagged by Moderation API.")
        return "Sorry, we cannot provide this information."

    logging.debug("Step 3: Response passed moderation check.")
    
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
    logging.debug("Step 4: Model evaluated if the response is a python code.")
        
    #Step 7: If yes, use this answer; if not, say that you cannot provide the information
    if "Y" in evaluation_response:  # Using "in" instead of "==" to be safer for model output variation (e.g., "Y." or "Yes")
        logging.debug("Step 5: Model approved the response.")
    else:
        logging.debug("Step 5: Model disapproved the response.")
        neg_str = "I'm unable to provide the information you're looking for."
        return neg_str

    logging.debug(final_response)
    
    #Step 8: Format and run the code and return output
    output_value = exec_response(final_response)
    logging.debug("Step 7: Executed generated python code.")
    return output_value


creds = authenticate_google_drive()
drive_service = build('drive', 'v3', credentials=creds)

question_input = st.text_input("Question:")
logging.info(f"Question: {question_input}")


if question_input:
        response = process_user_message(question_input)
else:
    response = ""
    
folder_id = '1aj2VjXTW_Tv2eti38HgjIUkc7PQQmQSw'
upload_file_to_drive('app.log', folder_id, drive_service)



st.text_area("Answer:", response, height=300)
