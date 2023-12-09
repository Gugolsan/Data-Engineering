import duckdb
import os


def create_table(con):
    con.sql("""CREATE TABLE electric_cars (vin VARCHAR, country VARCHAR, city VARCHAR, state VARCHAR, postal_code VARCHAR, 
                        model_year INTEGER, make VARCHAR, model VARCHAR, elecrtic_vehicle_type VARCHAR, cafv_eligibility VARCHAR,
                        electric_range INTEGER, base_msrp INTEGER, legislative_district VARCHAR, dol_vehicle_id VARCHAR, vehicle_location VARCHAR,
                        electric_utility VARCHAR, census_2020_tract VARCHAR)""")


def add_data(con, input_path):
    con.sql(f"INSERT INTO electric_cars SELECT * FROM '{input_path}'")


def count_cars_per_city(con, output_path):
    # Execute the SQL query to count the number of electric cars per city
    con.execute(f'''
        COPY (
            SELECT city, COUNT(*) AS car_count
            FROM electric_cars
            GROUP BY city
            ORDER BY car_count DESC
        ) TO '{output_path}' (FORMAT PARQUET)
    ''')


def find_top_3_popular_vehicles(con, output_path):
    # Execute the SQL query to find the top 3 most popular electric vehicles based on make and model
    con.execute(f''' 
        COPY ( 
            SELECT CONCAT_WS(' ', make, model) AS vehicle, COUNT(*) AS car_count
            FROM electric_cars
            GROUP BY make, model
            ORDER BY car_count DESC
            LIMIT 3
        ) TO '{output_path}' (FORMAT PARQUET)
    ''')


def find_most_popular_vehicle_per_postal_code(con, output_path):
    # Execute the SQL query to find the most popular electric vehicle in each postal code
    con.execute(f'''
        COPY ( 
            WITH temp AS ( SELECT postal_code, CONCAT_WS(' ', make, model) AS vehicle, COUNT(*) AS car_count, 
            ROW_NUMBER() OVER (PARTITION BY postal_code ORDER BY COUNT(*) DESC) AS rank FROM electric_cars GROUP BY 
            postal_code, make, model ) SELECT postal_code, vehicle, car_count FROM temp WHERE rank=1 
        ) TO '{output_path}' (FORMAT PARQUET)
    ''')


def count_cars_by_model_year(con, output_path):
    # Execute the SQL query to count the number of electric cars by model year
    con.execute(f'''
        COPY (
            SELECT model_year, COUNT(*) AS car_count
            FROM electric_cars
            GROUP BY model_year
            ORDER BY model_year
        ) TO '{output_path}' (FORMAT PARQUET, PARTITION_BY (model_year), OVERWRITE_OR_IGNORE 1)
    ''')
    # rows = result.fetchall()
    # for row in rows:
    #     print(f"Postal Code: {row[0]}")


def main():
    # Get the current working directory
    current_dir = os.getcwd()

    # Define the relative path to the 'data' directory
    relative_data_dir = "data"

    # Construct the absolute path to the 'data' directory
    csv_file_path = os.path.join(current_dir, relative_data_dir, 'electric-cars.csv')

    print(csv_file_path)
    # # Get the directory of the script
    # script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create a DuckDB connection
    con = duckdb.connect(database=':memory:', read_only=False)

    # Create table with the DDL
    create_table(con)

    # Insert data into created table
    add_data(con, csv_file_path)

    # Query the data to verify
    # result = con.execute('SELECT * FROM electric_cars LIMIT 5')
    # print(result.fetchall())

    # Output the result in results dir
    results_dir = os.path.join(current_dir, "results")
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    # Define the output paths for the Parquet files
    output_path1 = os.path.join(results_dir, 'count_cars_per_city.parquet')
    output_path2 = os.path.join(results_dir, 'find_top_3_popular_vehicle.parquet')
    output_path3 = os.path.join(results_dir, 'find_most_popular_vehicle_per_postal_code.parquet')
    output_path4 = os.path.join(results_dir, 'count_cars_by_model_year')

    # Call the function to count cars per city
    count_cars_per_city(con, output_path1)

    # Call the function to find the top 3 most popular electric vehicles
    find_top_3_popular_vehicles(con, output_path2)

    # Call the function to find the most popular electric vehicle in each postal code
    find_most_popular_vehicle_per_postal_code(con, output_path3)

    # Call the function to count cars by model year
    count_cars_by_model_year(con, output_path4)


if __name__ == "__main__":
    main()