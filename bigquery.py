from google.cloud import bigquery
from google.oauth2 import service_account
from getpass import getpass

# Initialize a BigQuery client with service account credentials
credentials = service_account.Credentials.from_service_account_file('theta-cell-406519-9ca186c504f4.json')
project_id = 'theta-cell-406519'
client = bigquery.Client(credentials=credentials, project=project_id)

# Define the BigQuery dataset and table names
dataset_name = 'project_stock_market'  # Replace with your dataset name
users_table_name = 'users'  # Replace with your users table name
tcs_table_name = 'TCS'  # Replace with your tcs table name

# Define the schema for the BigQuery users table
users_table_schema = [
    bigquery.SchemaField("id", "INT64"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("password", "STRING"),
    bigquery.SchemaField("role", "STRING"),
]

# Define the schema for the BigQuery tcs table
tcs_table_schema = [
    bigquery.SchemaField("Date", "STRING"),
    bigquery.SchemaField("Prev_Close", "FLOAT64"),
    bigquery.SchemaField("Open", "FLOAT64"),
    bigquery.SchemaField("High", "FLOAT64"),
    bigquery.SchemaField("Low", "FLOAT64"),
    bigquery.SchemaField("Close", "FLOAT64"),
    bigquery.SchemaField("VWAP", "FLOAT64"),
    bigquery.SchemaField("Volume", "INT64"),
    bigquery.SchemaField("Turnover", "FLOAT64"),
    bigquery.SchemaField("Deliverable_Volume", "INT64"),
    bigquery.SchemaField("Perc_Deliverable", "FLOAT64"),
    bigquery.SchemaField("Year_N", "INT64"),
]

# Create a BigQuery dataset (if not exists)
dataset_id = f"{project_id}.{dataset_name}"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"  # Replace with your desired location
try:
    client.create_dataset(dataset, timeout=30)
except Exception as e:
    print(f"Error creating dataset: {e}")

# Create a BigQuery users table (if not exists)
users_table_id = f"{dataset_id}.{users_table_name}"
users_table = bigquery.Table(users_table_id, schema=users_table_schema)
try:
    client.create_table(users_table, timeout=30)
except Exception as e:
    print(f"Error creating users table: {e}")

# Create a BigQuery tcs table (if not exists)
tcs_table_id = f"{dataset_id}.{tcs_table_name}"
tcs_table = bigquery.Table(tcs_table_id, schema=tcs_table_schema)
try:
    client.create_table(tcs_table, timeout=30)
except Exception as e:
    print(f"Error creating tcs table: {e}")

# Function to register a new user
def register_user():
    username = input("Enter your username: ")
    password = getpass("Enter your password: ")

    try:
        query = f"INSERT INTO {users_table_id} (username, password, role) VALUES ('{username}', '{password}', 'normal_user')"
        client.query(query)

        print("User registration successful! Role: normal_user")
    except Exception as e:
        print(f"Error: {e}\nUser registration failed.")

# Function to register a new admin
def register_admin():
    admin_username = input("Enter admin username: ")
    admin_password = getpass("Enter admin password: ")

    try:
        query = f"INSERT INTO {users_table_id} (username, password, role) VALUES ('{admin_username}', '{admin_password}', 'iam_admin')"
        client.query(query)

        print("Admin registration successful! Role: iam_admin")
    except Exception as e:
        print(f"Error: {e}\nAdmin registration failed.")

# Function for normal user login
def normal_user_login():
    username = input("Enter your username: ")
    password = getpass("Enter your password: ")

    query = f"SELECT * FROM {users_table_id} WHERE username='{username}' AND password='{password}' AND role='normal_user'"
    results = client.query(query)

    user = list(results)

    if user:
        print(f"Login successful! Role: {user[0]['role']}")
    else:
        print("Invalid username or password.")

# Function for admin login and CRUD operations
def admin_login():
    admin_username = input("Enter admin username: ")
    admin_password = getpass("Enter admin password: ")

    query = f"SELECT * FROM {users_table_id} WHERE username='{admin_username}' AND password='{admin_password}' AND role='iam_admin'"
    results = client.query(query)

    admin = list(results)

    if admin:
        print("Admin login successful!")

        # Perform CRUD operations
        while True:
            print("\n1. Create Record")
            print("2. Read Records")
            print("3. Update Record")
            print("4. Delete Record")
            print("5. Back")

            operation = input("Select an operation (1/2/3/4/5): ")

            if operation == '1':
                create_record()
            elif operation == '2':
                read_records()
            elif operation == '3':
                update_record()
            elif operation == '4':
                delete_record()
            elif operation == '5':
                break
            else:
                print("Invalid operation. Please enter 1, 2, 3, 4, or 5.")

    else:
        print("Invalid admin username or password.")

# CRUD operations on tcs table
def create_record():
    date = input("Enter Date: ")
    prev_close = float(input("Enter Prev_Close: "))
    open_value = float(input("Enter Open: "))
    high = float(input("Enter High: "))
    low = float(input("Enter Low: "))
    close = float(input("Enter Close: "))
    vwap = float(input("Enter VWAP: "))
    volume = int(input("Enter Volume: "))
    turnover = float(input("Enter Turnover: "))
    deliverable_volume = int(input("Enter Deliverable_Volume: "))
    perc_deliverable = float(input("Enter Perc_Deliverable: "))
    year_n = int(input("Enter Year_N: "))
# id,Date,      Prev_Close,Open,High,   Low, Close,  VWAP,   Volume, Turnover, Deliverable_Volume,Perc_Deliverable,Year_N
#   ,30-04-2021,3115.25,   3099,3132.05,3020,3035.65,3063.19,3072305,9.41E+14, 1942473,           0.6323,          18
    try:
        query = f"INSERT INTO {tcs_table_id} (Date, Prev_Close, Open, High, Low, Close, VWAP, Volume, Turnover, Deliverable_Volume, Perc_Deliverable, Year_N) VALUES ('{date}', {prev_close}, {open_value}, {high}, {low}, {close}, {vwap}, {volume}, {turnover}, {deliverable_volume}, {perc_deliverable}, {year_n})"
        client.query(query)

        print("Record added successfully!")
    except Exception as e:
        print(f"Error: {e}\nRecord creation failed.")


def read_records():
    query = f"SELECT * FROM {tcs_table_id}"
    results = client.query(query)

    records = list(results)

    for record in records:
        print(record)
    else:
        print("No records found in the table.")

def update_record():
    record_id = int(input("Enter the ID of the record to update: "))
    new_prev_close = float(input("Enter the new Prev_Close: "))
    new_open = float(input("Enter the new Open: "))
    new_high = float(input("Enter the new High: "))
    new_low = float(input("Enter the new Low: "))
    new_close = float(input("Enter the new Close: "))
    new_vwap = float(input("Enter the new VWAP: "))
    new_volume = int(input("Enter the new Volume: "))
    new_turnover = float(input("Enter the new Turnover: "))
    new_deliverable_volume = int(input("Enter the new Deliverable_Volume: "))
    new_perc_deliverable = float(input("Enter the new Perc_Deliverable: "))
    new_year_n = int(input("Enter the new Year_N: "))

    try:
        query = f"UPDATE {tcs_table_id} SET Prev_Close={new_prev_close}, Open={new_open}, High={new_high}, Low={new_low}, Close={new_close}, VWAP={new_vwap}, Volume={new_volume}, Turnover={new_turnover}, Deliverable_Volume={new_deliverable_volume}, Perc_Deliverable={new_perc_deliverable}, Year_N={new_year_n} WHERE id={record_id}"
        client.query(query)

        print("Record updated successfully!")
    except Exception as e:
        print(f"Error: {e}\nRecord update failed.")

def delete_record():
    record_id = int(input("Enter the ID of the record to delete: "))

    try:
        query = f"DELETE FROM {tcs_table_id} WHERE id={record_id}"
        client.query(query)

        print("Record deleted successfully!")
    except Exception as e:
        print(f"Error: {e}\nRecord deletion failed.")

# Main program
while True:
    print("\n1. Register User")
    print("2. Register Admin")
    print("3. Admin Login")
    print("4. Normal User Login")
    print("5. Exit")

    choice = input("Select an option (1/2/3/4/5): ")

    if choice == '1':
        register_user()
    elif choice == '2':
        register_admin()
    elif choice == '3':
        admin_login()
    elif choice == '4':
        normal_user_login()
    elif choice == '5':
        break
    else:
        print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")
