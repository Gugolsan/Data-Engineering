from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, weekofyear, month, dayofmonth, count, sum, row_number, rank, desc, dayofweek, concat_ws, when, sum as sql_sum, count as sql_count, sum as new_sql_sum, count as new_sql_count
from pyspark.sql.window import Window
import os
from pathlib import Path
import zipfile


# os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Админ\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Админ\\AppData\\Local\\Programs\\Python\\Python311\\python.exe'


def read_data(spark, file_path):
    archive = zipfile.ZipFile(file_path)
    csv_str = str(archive.read(Path(file_path).stem + '.csv'), 'utf-8')
    return spark.read.csv(spark.sparkContext.parallelize(csv_str.splitlines(), numSlices=1000), header=True)


def calculate_average_trip_duration_per_day(df):
    # Extract day from start_time
    df = df.withColumn('start_day', col('start_time').cast('date'))

    # Cast the tripduration column to double type
    #df = df.withColumn('tripduration', regexp_replace('tripduration', '\"', ''))

    # # Remove double quotes from tripduration and convert it to a double type
    # df = df.withColumn('tripduration_cleaned', expr('CAST(regexp_replace(tripduration, \'"\', \'\') AS DOUBLE)'))

    # Group by start_day and calculate the average trip duration
    result = df.groupBy('start_day').agg({'tripduration': 'avg'}).withColumnRenamed('avg(tripduration)', 'average_trip_duration')

    return result


def count_trips_per_day(df):
    # Extract day from start_time
    df = df.withColumn('start_day', col('start_time').cast('date'))

    # Group by start_day and count the number of trips
    result = df.groupBy('start_day').agg({'trip_id': 'count'}).withColumnRenamed('count(trip_id)', 'trips_taken')

    return result


def most_popular_starting_station_per_month(df):
    # Extract year and month from start_time
    df = df.withColumn('year', year(col('start_time')))
    df = df.withColumn('month', month(col('start_time')))

    # Group by year, month, and from_station_name, and count the number of trips
    result = df.groupBy('year', 'month', 'from_station_name').agg(count('trip_id').alias('count'))

    # Create a window partitioned by year and month, and ordered by count in descending order
    window_spec = Window.partitionBy('year', 'month').orderBy(col('count').desc())

    # Add a rank column based on the window
    result = result.withColumn('rank', row_number().over(window_spec))

    # Filter the top-ranked station for each month
    result = result.filter(col('rank') == 1).select('year', 'month', 'from_station_name', 'count').withColumnRenamed('from_station_name', 'most_popular_starting_station')

    return result


def top_trip_stations_per_day_last_two_weeks(df):
    # Extract date, week_of_year, and combine from_station_name and to_station_name
    df = df.withColumn('date', col('start_time').cast('date'))
    df = df.withColumn('week_of_year', weekofyear(df.start_time))
    df = df.withColumn('combined_stations', concat_ws('_', col('from_station_name'), col('to_station_name')))

    # Group by date, week_of_year, and combined_stations, count the number of trips
    result = df.groupBy('date', 'week_of_year', 'combined_stations').agg({'*': 'count'}).withColumnRenamed('count(1)', 'count')

    # Use window function to get the top 3 stations for each day and week
    window_spec = Window.partitionBy('date', 'week_of_year').orderBy(col('count').desc())
    result = result.withColumn('rank', row_number().over(window_spec))
    result = result.filter(col('rank') <= 3).select('date', 'week_of_year', 'combined_stations', 'count')

    # Filter for the two largest week_of_year values
    max_weeks = result.select('week_of_year').distinct().orderBy(col('week_of_year').desc()).limit(2)
    result = result.join(max_weeks, 'week_of_year', 'inner')

    return result


def average_trip_duration_by_gender(df):
    # Filter out rows where gender is not specified
    df_filtered = df.filter((col('gender').isNotNull()) & (col('gender') != ''))

    # Group by gender and calculate the sum and count of trip durations
    result = df_filtered.groupBy('gender').agg(
        sql_sum('tripduration').alias('total_trip_duration'),
        sql_count('trip_id').alias('total_trips')
    )

    # Calculate the average trip duration for each gender
    result = result.withColumn('average_trip_duration', col('total_trip_duration') / col('total_trips'))

    return result


def top_ages_longest_shortest_trips(df):
    # Filter out rows where birthyear is not specified
    df_filtered = df.filter((col('birthyear').isNotNull()) & (col('birthyear') != '') & (col('birthyear').cast('int').isNotNull()))

    # Filter out rows where tripduration is not specified
    df_filtered = df_filtered.filter((col('tripduration').isNotNull()) & (col('tripduration') != '') & (col('tripduration').cast('double').isNotNull()))

    # Extract the year from start_time
    df_filtered = df_filtered.withColumn('year', year(col('start_time')))
    
    # Calculate age
    age = col('year') - col('birthyear')

    # Add the age column to the DataFrame
    df_with_age = df_filtered.withColumn('age', age)

    # Group by age and calculate the sum and count of trip durations
    result = df_with_age.groupBy('age').agg(
       sum('tripduration').alias('total_trip_duration'),
       count('trip_id').alias('total_trips')
    )

    # Calculate the average trip duration for each age
    result = result.withColumn('average_trip_duration', col('total_trip_duration') / col('total_trips'))

    # Order by average_trip_duration to get top 10 longest and shortest
    result_longest = result.orderBy(col('average_trip_duration').desc()).limit(10)
    result_shortest = result.orderBy(col('average_trip_duration')).limit(10)

    return result_longest, result_shortest


def main():
    spark = SparkSession.builder.appName("Lab5").enableHiveSupport().getOrCreate()
    # Define data directory dynamically
    current_dir = os.getcwd()
    data_dir = os.path.join(current_dir, "data")

    # # Read the zip file from the data folder
    # zips = spark.sparkContext.binaryFiles("data/Divvy_Trips_2019_Q4.zip")

    # Convert the files_data RDD to a Spark DataFrame
    df1 = read_data(spark, os.path.join(data_dir, "Divvy_Trips_2019_Q3.zip"))
    df2 = read_data(spark, os.path.join(data_dir, "Divvy_Trips_2019_Q4.zip"))
    
    # Get the top trip stations per day for the last two weeks separately for each file
    df3 = top_trip_stations_per_day_last_two_weeks(df1)
    df4 = top_trip_stations_per_day_last_two_weeks(df2)

    # Calculate average trip duration by gender
    df5 = average_trip_duration_by_gender(df1)
    df6 = average_trip_duration_by_gender(df2)

    # Calculate top 10 longest and shortest trip durations for each age
    df7_longest, df7_shortest = top_ages_longest_shortest_trips(df1)
    df8_longest, df8_shortest = top_ages_longest_shortest_trips(df2)
    
    # Combine df1 and df2
    df = df1.union(df2)

    # Calculate average trip duration per day
    result1 = calculate_average_trip_duration_per_day(df)

    # Count trips per day
    result2 = count_trips_per_day(df)

    # Get the most popular starting station for each month
    result3 = most_popular_starting_station_per_month(df)

    # Combine the results from both files
    result4 = df3.union(df4)

    # Combine the results from both files
    result5 = df5.union(df6)

    # Combine the results from both files
    # result_longest, result_shortest = top_ages_longest_shortest_trips(df)
    result6 = df7_longest.union(df8_longest)
    result7 = df7_shortest.union(df8_shortest)

    # Display first 5 rows for the dataframe
    # print("First 5 rows of df1:")
    # result6.show(5, truncate=False)

    # Output the result as a report in CSV format
    reports_dir = os.path.join(current_dir, "reports")
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)

    result1.repartition(1).write.csv(os.path.join(reports_dir, "average_trip_duration_per_day_report.csv"), header=True, mode="overwrite")
    # result.repartition(1).write.csv(os.path.join(reports_dir, "average_trip_duration_report.csv"), header=True, mode="overwrite")
    result2.repartition(1).write.csv(os.path.join(reports_dir, "trips_per_day_report.csv"), header=True, mode="overwrite")
    result3.repartition(1).write.csv(os.path.join(reports_dir, "most_popular_starting_station_per_month_report.csv"), header=True, mode="overwrite")
    result4.repartition(1).write.csv(os.path.join(reports_dir, "top_trip_stations_last_two_weeks_report.csv"), header=True, mode="overwrite")
    result5.repartition(1).write.csv(os.path.join(reports_dir, "average_trip_duration_by_gender_report.csv"), header=True, mode="overwrite")
    result6.repartition(1).write.csv(os.path.join(reports_dir, "top_10_longest_trips_by_age_report.csv"), header=True, mode="overwrite")
    result7.repartition(1).write.csv(os.path.join(reports_dir, "top_10_shortest_trips_by_age_report.csv"), header=True, mode="overwrite")

if __name__ == "__main__":
    main()
