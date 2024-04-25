# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime
import string
import random
from typing import List
from pyspark.sql import DataFrame


def generate_id(N=4):
    random_string = "".join(random.choices(string.ascii_uppercase + string.digits, k=N))
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{random_string}_{now_str}"


def join_check(joins_file, column):
    joins_file = joins_file.filter(joins_file.column_dim == column)

    df_fact = joins_file.select("table_fact").rdd.flatMap(lambda x: x).collect()
    column_fact = joins_file.select("column_fact").rdd.flatMap(lambda x: x).collect()
    df_dim = joins_file.select("table_dim").rdd.flatMap(lambda x: x).collect()
    column_dim = joins_file.select("column_dim").rdd.flatMap(lambda x: x).collect()

    if joins_file.count() == 0:
        return "", "", ""
    else:
        try:  # try to perfome the test, if colmuns are not in DataFrame returns empty
            for i in range(joins_file.count()):

                # Fact
                count_fact = globals()[df_fact[i]].select(column_fact[i]).count()
                df1 = (
                    globals()[df_fact[i]]
                    .select(column_fact[i])
                    .withColumnRenamed(column_fact[i], "key")
                )
                # Dimension
                df2 = (
                    globals()[df_dim[i]]
                    .select(column_dim[i])
                    .withColumnRenamed(column_dim[i], "key")
                    .withColumn("check", F.lit("OK"))
                    .distinct()
                )

                # Result
                result_df = df1.join(df2, how="left", on="key").filter("check is null")
                result = result_df.count()
                if result > 0:
                    sample_join = result_df.select("key").collect()[0][0]
                else:
                    sample_join = ""
                return (
                    str(result),
                    str(float("{:.2f}".format(result / count_fact))),
                    sample_join,
                )
        except:
            return "", "", ""


def kpi_allocation(df, column):
    df = df.withColumn("discounts_tot", F.col("discounts_tot").cast("long"))
    df = df.withColumn(
        "total_order_volume_hl", F.col("total_order_volume_hl").cast("long")
    )

    volume_total = df.groupBy().sum("total_order_volume_hl").collect()[0][0]
    discount_total = df.groupBy().sum("discounts_tot").collect()[0][0]

    df_allocation = (
        df.groupBy(column)
        .sum("discounts_tot", "total_order_volume_hl")
        .withColumn(
            "discounts_perc",
            F.round((F.col("sum(discounts_tot)") / discount_total) * 100, 4),
        )
        .withColumn(
            "volume_perc",
            F.round((F.col("sum(total_order_volume_hl)") / volume_total) * 100, 4),
        )
    )

    if df_allocation.filter(f"{column} is null").count() == 0:
        return "0", "0"
    else:  # Discounts/Volume
        return str(df_allocation.filter(f"{column} is null").collect()[0][3]), str(
            df_allocation.filter(f"{column} is null").collect()[0][4]
        )


def check_column(
    df,
    column,
    data_type,
    table_name,
    fact_table,
    primary_key,
    foreign_key,
    mandatory,
    joins_file,
):

    # Number of records
    if column in df.columns:
        size = df.count()
        size_result = str(size)
    else:
        size_result = ""

    # Number of records without duplicates
    if column in df.columns:
        size_without_duplicates = (
            df.groupBy(df.columns).count().filter("count = 1").count()
        )
        size_without_dup_result = str(size_without_duplicates)
    else:
        size_without_dup_result = ""

    # Count distinct - Specific column
    if column in df.columns:
        count_distinct_column_num = df.select(column).distinct().count()
        count_distinct_column = str(count_distinct_column_num)
        count_distinct_column_perc = str(
            float("{:.2f}".format(count_distinct_column_num / size))
        )
    else:
        count_distinct_column = ""
        count_distinct_column_perc = ""

    # Count null - Specific column
    if column in df.columns:
        result_null_num = (
            df.select(column)
            .withColumnRenamed(column, "key")
            .withColumn(
                "null_check",
                F.expr(
                    """
          case 
          when key is null or key in ('', '-', 'NA' , 'NI' , 'ND' , 'x', 'NOT APPLICABLE' , 'NOT INFORMED', 'NOT DETERMINED',  'NULL' , 'NO INFORMADO' , 'N/A' , 'N/D' , 'N/I', '-99') then 1
          else 0 
          end"""
                ),
            )
            .filter("null_check != 0")
            .count()
        )
        result_null = str(result_null_num)
        result_null_perc = str(float("{:.2f}".format(result_null_num / size)))
    else:
        result_null = ""
        result_null_perc = ""

    # Duplicates values in a column
    if column in df.columns:
        result_duplicate = str(df.groupBy(column).count().filter("count > 1").count())
    else:
        result_duplicate = ""

    # Date check
    if column in df.columns:
        if data_type == "date":
            result_date_df = (
                df.select(column)
                .withColumnRenamed(column, "key")
                .withColumn(
                    "date_check",
                    F.expr(
                        "case when key is not null and cast(key as timestamp) is null then 1 else 0 end"
                    ),
                )
                .filter("date_check != 0")
                .count()
            )
            if result_date_df == 0:
                result_date = str(result_date_df)  # Success
            else:
                result_date = str(result_date_df)  # False
        else:
            result_date = ""
    else:
        result_date = ""

    # Value check
    if column in df.columns:
        if data_type == "value":
            result_value_df = (
                df.select(column)
                .withColumnRenamed(column, "key")
                .withColumn(
                    "value_check",
                    F.expr(
                        "case when key is not null and CAST(key AS DECIMAL(24,6)) is null then 1 else 0 end"
                    ),
                )
                .filter("value_check != 0")
                .count()
            )
            if result_value_df == 0:
                result_value = str(result_value_df)  # Success
            else:
                result_value = str(result_value_df)  # Failure
        else:
            result_value = ""
    else:
        result_value = ""

    # Mandatory
    if column in df.columns:
        result_mandatory = "Success"
    else:
        result_mandatory = "Failure"

    # Joins
    if column in df.columns and fact_table == "FALSE" and primary_key == "TRUE":
        distinct_key_missing, distinct_key_missing_perc, sample_join = join_check(
            joins_file, column
        )
    else:
        distinct_key_missing = ""
        distinct_key_missing_perc = ""
        sample_join = ""

    # KPI Allocation
    if column in df.columns and fact_table == "TRUE" and foreign_key == "TRUE":
        discounts_allocation, volume_allocation = kpi_allocation(df, column)
    else:
        discounts_allocation = ""
        volume_allocation = ""

    # Result
    result = (
        table_name,
        column,
        data_type,
        fact_table,
        primary_key,
        foreign_key,
        mandatory,
        result_null,  # Quantity of Null
        result_null_perc,  # Null Percentage
        count_distinct_column,  # Quantity of distinct
        count_distinct_column_perc,  # Percentage of distinct
        size_result,  # Size
        size_without_dup_result,  # Size without duplicates
        result_duplicate,  # Duplicate
        distinct_key_missing,  # Number of keys missing in dimesion table
        distinct_key_missing_perc,  # Percentage of keys missing in fact table
        sample_join,  # Sample of key missing
        result_date,  # Result of cast as datetime
        result_value,  # Result of cast as value
        result_mandatory,  # If the field parameterized as mandatory is in the file
        discounts_allocation,
        volume_allocation,
    )

    return result


def main_function(path):
    batch_run_id = generate_id(N=4)

    # Read the JSON files and convert to Spark DataFrame
    parameter_file = spark.read.options(multiline=True, inferSchema=True).json(
        f"{path}column_tables.json"
    )
    joins_file = spark.read.options(multiline=True, inferSchema=True).json(
        f"{path}joins.json"
    )
    path_file = spark.read.options(multiline=True, inferSchema=True).json(
        f"{path}path.json"
    )
    path_file = path_file.withColumn(
        "full_path",
        F.concat(
            path_file.path,
            F.lit("country_cd="),
            path_file.country,
            F.lit("/extract_version_cd="),
            path_file.version,
            F.lit("/extract_dt="),
            path_file.date_path,
            F.lit("/"),
        ),
    )

    # Read the tables on JSON files
    for i in range(
        len(path_file.select("table_name").rdd.flatMap(lambda x: x).collect())
    ):
        version = path_file.select("version").rdd.flatMap(lambda x: x).collect()[i]
        table_name = (
            path_file.select("table_name").rdd.flatMap(lambda x: x).collect()[i]
        )
        path_file_str = (
            path_file.select("full_path").rdd.flatMap(lambda x: x).collect()[i]
        )

        if table_name == "invoice":
            globals()[
                path_file.select("table_name").rdd.flatMap(lambda x: x).collect()[i]
            ] = (
                spark.read.format(
                    path_file.select("table_format")
                    .rdd.flatMap(lambda x: x)
                    .collect()[i]
                )  # Table format
                .option("header", True)
                .option(
                    "delimiter",
                    path_file.select("delimiter").rdd.flatMap(lambda x: x).collect()[i],
                )  # Delimiter
                .load(path_file_str)
                .limit(500000)
            )

        else:
            globals()[
                path_file.select("table_name").rdd.flatMap(lambda x: x).collect()[i]
            ] = (
                spark.read.format(
                    path_file.select("table_format")
                    .rdd.flatMap(lambda x: x)
                    .collect()[i]
                )  # Table format
                .option("header", True)
                .option(
                    "delimiter",
                    path_file.select("delimiter").rdd.flatMap(lambda x: x).collect()[i],
                )  # Delimiter
                .load(path_file_str)
            )

    # Select table and column name
    table_name = parameter_file.select("table").rdd.flatMap(lambda x: x).collect()
    table_colum = parameter_file.select("column").rdd.flatMap(lambda x: x).collect()
    table_type = (
        parameter_file.select("data_type_final").rdd.flatMap(lambda x: x).collect()
    )
    fact_table = parameter_file.select("fact_table").rdd.flatMap(lambda x: x).collect()
    primary_key = (
        parameter_file.select("primary_key").rdd.flatMap(lambda x: x).collect()
    )
    foreign_key = (
        parameter_file.select("foreign_key").rdd.flatMap(lambda x: x).collect()
    )
    mandatory = parameter_file.select("mandatory").rdd.flatMap(lambda x: x).collect()

    # Testing and export columns result
    result_final = []
    for i in range(len(table_name)):
        result_final.append(
            check_column(
                globals()[table_name[i]],
                table_colum[i],
                table_type[i],
                table_name[i],
                fact_table[i],
                primary_key[i],
                foreign_key[i],
                mandatory[i],
                joins_file,
            )
        )

    # Convert columns result to a DataFrame
    df_result = spark.createDataFrame(
        data=result_final,
        schema=[
            "table_name",
            "column",
            "data_type",
            "fact_table",
            "primary_key",
            "foreign_key",
            "mandatory",
            "null_count",
            "null_percentage",
            "distinct_count",
            "distinct_percentage",
            "size",
            "size_without_dup",
            "duplicate_column",
            "distinct_key_missing",
            "distinct_key_missing_perc",
            "sample_key_missing",
            "date_check",
            "value_check",
            "mandatory_check",
            "discounts_allocation",
            "volume_allocation",
        ],
    )

    df_result = df_result.withColumn("date", F.lit(datetime.today().strftime("%Y%m%d")))
    df_result = df_result.withColumn("batch_run_id", F.lit(batch_run_id))
    df_result = df_result.join(
        path_file.select("table_name", "full_path"), on="table_name", how="left"
    )

    df_result.write.format("delta").option("header", "true").mode("append").save(
        "abfss://brewdat-ghq@brewdatadlsgbdev.dfs.core.windows.net/root/lz_data/non_src_sys/bees_engine/quality_checks/bronze_validation_v8"
    )

    return f"The results were successfully written to the table available on Microsoft Storage (batch_run_id = {batch_run_id})."


def validation_silver(
    df: DataFrame, key_column: str, data_dictionary_columns: List[str]
):
    result_duplicates = df.groupBy(key_column).count().filter("count > 1").count()

    result_null = (
        df.withColumn(
            "null_check",
            F.expr(
                f"""case when {key_column} is null or {key_column} in ('', '-', 'NA' , 'NI' , 'ND' , 'x', 'NOT APPLICABLE' , 'NOT INFORMED', 'NOT DETERMINED',  'NULL' , 'NO INFORMADO' , 'N/A' , 'N/D' , 'N/I', '-99') then 1 else 0 end"""
            ),
        )
        .filter("null_check = 1")
        .count()
    )

    result_layout = 0
    for column in data_dictionary_columns:
        if column in df.columns:
            result_layout = result_layout + 0
        else:
            result_layout = result_layout + 1
    try:

        if result_null != 0:
            raise Exception(f"There are {result_null} null values in {key_column}.")

        elif result_duplicates != 0:
            raise Exception(
                f"There are {result_duplicates} duplicates in {key_column}."
            )
        else:
            print("Data validation was successfully run.")

    except:
        common_utils.exit_with_last_exception()


def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            raise
