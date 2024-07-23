import pyodbc
import pandas as pd
import requests

# Configure your DSN and connection details
dsn = 'Your_Sage_X3_DSN'
username = 'your_username'
password = 'your_password'
database = 'your_database'

# Establish connection to Sage X3
conn = pyodbc.connect(f'DSN={dsn};UID={username};PWD={password};DATABASE={database}')

# SQL query to fetch order data
query = '''
SELECT 
    order_id AS 'OrderNumber',
    customer_name AS 'RecipientName',
    customer_address AS 'AddressLine1',
    customer_city AS 'City',
    customer_state AS 'State',
    customer_zip AS 'PostalCode',
    customer_country AS 'Country',
    shipping_method AS 'Service'
FROM 
    sage_x3_orders
WHERE 
    shipping_status = 'Pending';
'''

# Fetch data and load into a pandas DataFrame
df = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Display the data
print(df.head())

# Data transformation if necessary
df['Service'] = df['Service'].map({
    'Standard': 'GROUND',
    'Express': 'NEXT_DAY_AIR'
    # Add other mappings as needed
})

# Save to a CSV file if UPS WorldShip can import CSV files
df.to_csv('orders_for_ups.csv', index=False)

def send_to_ups(data):
    # UPS WorldShip API endpoint
    url = 'https://www.ups.com/ship/v1/shipments'
    
    # API credentials
    api_key = 'your_ups_api_key'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    
    # Convert DataFrame to JSON
    data_json = data.to_json(orient='records')
    
    # Send data to UPS WorldShip
    response = requests.post(url, headers=headers, data=data_json)
    
    if response.status_code == 200:
        print('Data sent successfully.')
    else:
        print(f'Failed to send data: {response.text}')

# Send data
send_to_ups(df)

