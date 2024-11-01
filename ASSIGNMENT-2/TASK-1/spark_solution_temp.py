from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, upper
from pyspark.sql import functions as F
from pyspark.sql.types import StringType  # Import StringType to check for string columns
import sys

# Initialize SparkSession
spark = SparkSession.builder.appName("Task1").getOrCreate()

# Define command line arguments for input files
athlete_2012 = sys.argv[1]
athlete_2016 = sys.argv[2]
athlete_2020 = sys.argv[3]
coaches = sys.argv[4]
medals = sys.argv[5]
output = sys.argv[6]

# Load the data from CSV files and append the year column
athlete_2012_df = spark.read.csv(athlete_2012, header=True, inferSchema=True).withColumn("Year", F.lit(2012))
athlete_2016_df = spark.read.csv(athlete_2016, header=True, inferSchema=True).withColumn("Year", F.lit(2016))
athlete_2020_df = spark.read.csv(athlete_2020, header=True, inferSchema=True).withColumn("Year", F.lit(2020))

medals_df = spark.read.csv(medals, header=True, inferSchema=True)
coaches_df = spark.read.csv(coaches, header=True, inferSchema=True)
coaches_df = coaches_df.drop('age', 'years_of_experience', 'contract_id', 'certification_committee')
coaches_df= coaches_df.withColumnRenamed("country", "COACH_COUNTRY")

# Combine athlete data across years
all_athletes_df = athlete_2012_df.union(athlete_2016_df).union(athlete_2020_df)
all_athletes_df=all_athletes_df.drop("dob","height","weight","num_followers","num_articles","personal_best")

# Filter medals for the required years
medals_filtered_df = medals_df.filter(col("year").isin(2012, 2016, 2020))

# Rename sport and id columns in medals to avoid conflict
medals_filtered_df = medals_filtered_df.withColumnRenamed("sport", "medal_sport")
medals_filtered_df = medals_filtered_df.withColumnRenamed("id", "medal_id")
medals_filtered_df = medals_filtered_df.withColumnRenamed("event", "medal_event")

# Convert all string columns to uppercase for both athletes and medals datasets
# Use StringType from pyspark.sql.types to check the data type of columns
all_athletes = [field.name for field in all_athletes_df.schema.fields if isinstance(field.dataType, StringType)]
string_cols_medals = [field.name for field in medals_filtered_df.schema.fields if isinstance(field.dataType, StringType)]

for col_name in all_athletes:
    all_athletes_df = all_athletes_df.withColumn(col_name, upper(col(col_name)))

for col_name in string_cols_medals:
    medals_filtered_df = medals_filtered_df.withColumn(col_name, upper(col(col_name)))

# Calculate points based on the year and medal type
medals_with_points = medals_filtered_df.withColumn(
    "points",
    F.when((col("year") == 2012) & (col("medal") == "GOLD"), 20)
    .when((col("year") == 2012) & (col("medal") == "SILVER"), 15)
    .when((col("year") == 2012) & (col("medal") == "BRONZE"), 10)
    .when((col("year") == 2016) & (col("medal") == "GOLD"), 12)
    .when((col("year") == 2016) & (col("medal") == "SILVER"), 8)
    .when((col("year") == 2016) & (col("medal") == "BRONZE"), 6)
    .when((col("year") == 2020) & (col("medal") == "GOLD"), 15)
    .when((col("year") == 2020) & (col("medal") == "SILVER"), 12)
    .when((col("year") == 2020) & (col("medal") == "BRONZE"), 7)
    .otherwise(0)
)

# Join athletes with medals based on matching 'id' and 'sport'/'medal_sport'
athletes_medals_joined = all_athletes_df.join(
    medals_with_points, 
    (all_athletes_df["id"] == medals_with_points["medal_id"]) &
    (all_athletes_df["sport"] == medals_with_points["medal_sport"]) &
    (all_athletes_df["Year"] == medals_with_points["year"]) &
    (all_athletes_df["event"] == medals_with_points["medal_event"])
)

# Select relevant columns like 'id', 'name', 'medal_sport', and 'points'
athlete_points_with_names = athletes_medals_joined.select("id", "name", "medal_sport", "points", "medal")

# Calculate total points and medal counts per athlete in each sport
athlete_total_points = athlete_points_with_names.groupBy("id", "name", "medal_sport") \
    .agg(
        F.sum("points").alias("total_points"),
        F.sum(F.when(col("medal") == "GOLD", 1).otherwise(0)).alias("gold_count"),
        F.sum(F.when(col("medal") == "SILVER", 1).otherwise(0)).alias("silver_count"),
        F.sum(F.when(col("medal") == "BRONZE", 1).otherwise(0)).alias("bronze_count")
    )

# Order the result by total points, gold count, silver count, bronze count, and name
sorted_sport_calc = athlete_total_points.orderBy(
    col("medal_sport").asc(),
    col("total_points").desc(),
    col("gold_count").desc(),
    col("silver_count").desc(),
    col("bronze_count").desc(),
    col("name")  # lexicographic order for ties
)

# Keep only the first entry for each sport
first_entry_per_sport = sorted_sport_calc.dropDuplicates(["medal_sport"])
all_athletes_df = all_athletes_df.filter(col("country").isin("CHINA", "INDIA", "USA"))

coaches_df = coaches_df.withColumnRenamed("id", "coach_unique_id")
coaches_df = coaches_df.withColumnRenamed("name", "coach_unique_name")
coaches_df = coaches_df.withColumnRenamed("sport", "coach_unique_sport")


string_cols_coaches = [field.name for field in coaches_df.schema.fields if isinstance(field.dataType, StringType)]
for col_name in string_cols_coaches:
    coaches_df = coaches_df.withColumn(col_name, upper(col(col_name)))
# Write the output to the file in the specified format

athletes_coaches_joined = all_athletes_df.join(
    coaches_df,
    (all_athletes_df["coach_id"] == coaches_df["coach_unique_id"]) &  # Match on coach_id from both DataFrames
    (all_athletes_df["sport"] == coaches_df["coach_unique_sport"])  # Match on sport from both DataFrames
)
athletes_coaches_joined=athletes_coaches_joined.withColumnRenamed("id", "athelete_id")
athletes_coaches_joined=athletes_coaches_joined.withColumnRenamed("name", "temp")
athletes_coaches_joined=athletes_coaches_joined.withColumnRenamed("country", "temp_country")
joined_df = athletes_coaches_joined.join(medals_with_points, 
    (athletes_coaches_joined["athelete_id"] == medals_with_points["medal_id"]) &
    (athletes_coaches_joined["sport"] == medals_with_points["medal_sport"]) &
    (athletes_coaches_joined["Year"] == medals_with_points["year"]) &
    (athletes_coaches_joined["event"] == medals_with_points["medal_event"]), "inner")

grouped_df = joined_df.groupBy("temp_country", "coach_unique_name", "coach_unique_id").agg(
    F.sum("points").alias("total_points"),
    F.sum(F.when(col("medal") == "GOLD", 1).otherwise(0)).alias("gold_count"),
    F.sum(F.when(col("medal") == "SILVER", 1).otherwise(0)).alias("silver_count"),
    F.sum(F.when(col("medal") == "BRONZE", 1).otherwise(0)).alias("bronze_count")
)
grouped_df = grouped_df.orderBy(
    "temp_country",              # Order by country
    F.desc("total_points"),      # Descending order of total points
    F.desc("gold_count"),        # Descending order of gold count
    F.desc("silver_count"),      # Descending order of silver count
    F.desc("bronze_count"),      # Descending order of bronze count
    "coach_unique_name"          # Lexicographic order of coach names
)

window_spec = Window.partitionBy("temp_country").orderBy(
    F.desc("total_points"), 
    F.desc("gold_count"), 
    F.desc("silver_count"), 
    F.desc("bronze_count"), 
    "coach_unique_name"
)
grouped_df = grouped_df.withColumn("row_num", F.row_number().over(window_spec))

# Step 3: Filter to get only the top 5 coaches (or less if not available) per country
top_coaches_df = grouped_df.filter(grouped_df["row_num"] <= 5)

# Step 4: Convert the filtered DataFrame to an RDD


ath_final = first_entry_per_sport.select('name').rdd.flatMap(lambda x: x).collect()
coach_final=top_coaches_df.select('coach_unique_name').rdd.flatMap(lambda x: x).collect()
athlete_names_str = '["' + '", "'.join(ath_final) + '"]'
coach_names_str = '["' + '", "'.join(coach_final) + '"]'
with open(output, 'w') as filedata:
    # Format the output as a nested list
    final_output = f'({athlete_names_str}, {coach_names_str})'
    filedata.write(final_output)
filedata.close()