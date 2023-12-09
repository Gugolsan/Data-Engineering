import psycopg2
import os
from io import BytesIO, StringIO


def create_tables(conn):
    # SQL logic for table creation
    create_statements = """
        CREATE TABLE accounts (
            customer_id INT PRIMARY KEY NOT NULL,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            address_1 VARCHAR(255),
            address_2 VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            zip_code VARCHAR(10),
            join_date DATE
        );

        CREATE INDEX idx_account_customer_id ON accounts(customer_id);

        CREATE TABLE products (
            product_id INT PRIMARY KEY NOT NULL,
            product_code VARCHAR(10),
            product_description VARCHAR(255)
        );

        CREATE INDEX idx_products_product_id ON products(product_id);

        CREATE TABLE transactions (
            transaction_id VARCHAR(50) PRIMARY KEY NOT NULL,
            transaction_date DATE,
            product_id INT,
            product_code VARCHAR(10),
            product_description VARCHAR(255),
            quantity INT,
            account_id INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
        );

        CREATE INDEX idx_transactions_transaction_id ON transactions(transaction_id);
    """

    # Create a cursor to interact with the database
    with conn.cursor() as cursor:
        # Execute the SQL logic to create tables
        cursor.execute(create_statements)

    # Print a success message
    print("Tables created successfully!")


def ingest_data(conn, accounts_path, products_path, transactions_path):
    # Create a cursor to interact with the database
    with conn.cursor() as cursor:
        # Open the CSV file for reading
        with open(accounts_path, 'r') as f:
            # Skip the header line
            next(f)
            # Read the contents of the file and replace ", " with ","
            data = f.read().replace(", ", ",")
            # Use the copy_from method to efficiently copy data from the file to the 'accounts' table
            cursor.copy_from(StringIO(data), 'accounts', sep=',')
        with open(products_path, 'r') as f:
            # Skip the header line
            next(f)
            # Read the contents of the file and replace ", " with ","
            data = f.read().replace(", ", ",")
            # Use the copy_from method to efficiently copy data from the file to the 'products' table
            cursor.copy_from(StringIO(data), 'products', sep=',')
        with open(transactions_path, 'r') as f:
            # Skip the header line
            next(f)
            # Read the contents of the file and replace ", " with ","
            data = f.read().replace(", ", ",")
            # Use the copy_from method to efficiently copy data from the file to the 'transactions' table
            cursor.copy_from(StringIO(data), 'transactions', sep=',')

    # Print a success message
    print("Data inserted successfully!")


def retrieve_data(conn):
    # SQL logic for SELECT query
    query_statement = """
	SELECT *
    FROM transactions
    JOIN accounts ON transactions.account_id = accounts.customer_id
    JOIN products ON transactions.product_id = products.product_id;
    """

    # Create a cursor to interact with the database
    with conn.cursor() as cursor:
        # Execute the SELECT query
        cursor.execute(query_statement)
        # Fetch all results and print each row
        for i in cursor.fetchall():
            print(i)


def drop_tables(conn):
    # SQL logic for table creation
    delete_statements = """
	DROP TABLE transactions;
        
	DROP TABLE accounts;
	
	DROP TABLE products;
    """

    # Create a cursor to interact with the database
    with conn.cursor() as cursor:
	# Execute the SQL logic to drop tables
        cursor.execute(delete_statements)
    # Print a success message
    print("Tables deleted successfully!")


def main():
    host = "postgres"  # We can use 'localhost', that indicates that the database server is on the same machine
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    # We also can add port, by default - 5432
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    # Get the current working directory
    current_dir = os.getcwd()

    # Define the relative path to the 'data' directory
    relative_data_dir = "data"

    # Construct the full path to the 'accounts.csv' file
    accounts_path = os.path.join(current_dir, relative_data_dir, 'accounts.csv')
    # Construct the full path to the 'products.csv' file
    products_path = os.path.join(current_dir, relative_data_dir, 'products.csv')
    # Construct the full path to the 'transactions.csv' file
    transactions_path = os.path.join(current_dir, relative_data_dir, 'transactions.csv')

    # Create tables
    create_tables(conn)

    # Delete tables
    # drop_tables(conn)

    # Add data
    ingest_data(conn, accounts_path, products_path, transactions_path)

    # View data
    # retrieve_data(conn)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
