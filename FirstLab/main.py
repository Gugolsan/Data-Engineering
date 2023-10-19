# from pyspark.sql import SparkSession
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("JSON modification").getOrCreate()
#
#     df = spark.read.json("10K.github.jsonl")
#
#     df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, \
    regexp_replace, lower, expr, split, concat_ws
import pandas

# Create a Spark session
spark = SparkSession.builder.appName("FilterAndSelectColumns").getOrCreate()

# Load the JSON file into a DataFrame
json_file_path = "10K.github.jsonl"
df = spark.read.json(json_file_path)

# Filter records where the 'type' field is 'PushEvent'
filtered_df = df.filter((col("type") == "PushEvent") & (size(col("payload.commits")) > 0))

# Explode the 'commits' array to multiple rows
exploded_df = filtered_df.withColumn("commit", explode(col("payload.commits")))

# Extract unique author names and corresponding messages
extracted_df = exploded_df.select("commit.author.name", "commit.message")

# Extract and transform author names and messages
transformed_df = exploded_df.select(lower(col("commit.author.name")).alias("author"),
                                    lower(regexp_replace(col("commit.message"), "[^a-zA-Z0-9\\s]", "")).alias(
                                        "message"))

# Replace multiple occurrences of '\n' with a single space
transformed_df = transformed_df.withColumn("message", regexp_replace(col("message"), "\\s+", " "))

# Filter out messages with less than 5 words
transformed_df = transformed_df.withColumn("word_count", size(split(col("message"), " ")))
transformed_df = transformed_df.where(col("word_count") >= 5).drop("word_count")

# Keep only the first 5 words in the 'message' column
transformed_df = transformed_df.withColumn("message", expr("substring_index(message, ' ', 5)"))

# Split the message into three trigram columns
transformed_df = transformed_df.withColumn("first_trigram", concat_ws(" ", split(col("message"), " ")[0],
                                                                      split(col("message"), " ")[1],
                                                                      split(col("message"), " ")[2]))
transformed_df = transformed_df.withColumn("second_trigram", concat_ws(" ", split(col("message"), " ")[1],
                                                                       split(col("message"), " ")[2],
                                                                       split(col("message"), " ")[3]))
transformed_df = transformed_df.withColumn("third_trigram", concat_ws(" ", split(col("message"), " ")[2],
                                                                      split(col("message"), " ")[3],
                                                                      split(col("message"), " ")[4]))

# # Generate trigrams using a sliding window approach
# transformed_df = transformed_df.withColumn("message",
#                                            concat_ws(", ",
#                                                      concat_ws(" ", split(col("message"), " ")[0],
#                                                                split(col("message"), " ")[1],
#                                                                split(col("message"), " ")[2]),
#                                                      concat_ws(" ", split(col("message"), " ")[1],
#                                                                split(col("message"), " ")[2],
#                                                                split(col("message"), " ")[3]),
#                                                      concat_ws(" ", split(col("message"), " ")[2],
#                                                                split(col("message"), " ")[3],
#                                                                split(col("message"), " ")[4])
#                                                      ))

# Show the filtered and selected DataFrame
transformed_df.show(truncate=False)

transformed_df.select("author", "first_trigram", "second_trigram", "third_trigram").\
    toPandas().to_csv('result.csv', index=False, header=False)
# # # Save the transformed DataFrame as a CSV file
# output_csv_path = "output.csv"
# transformed_df.write.option("header", False).option("delimiter", ",").csv(output_csv_path)

# Stop the Spark session
spark.stop()
