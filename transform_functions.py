# Databricks notebook source
# MAGIC %run ./read_functions

# COMMAND ----------

import requests, uuid
from datetime import datetime
from typing import Mapping, Union, Optional, Sequence
from pyspark.sql import Column, DataFrame
from cytoolz import curry, reduce
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

"""
    Azure Translate Functions.

    @author: paulo.silva@ab-inbev.com.
"""


def azure_translator(text_list, language_from="auto", language_to=["en"]) -> list:
    """

    Use Azure Translation API to perform translation.

    It is based on Azure Cognitive Services PaaS service which contains
    translation service.

    Parameters
    ----------
    text_list : list
        A list of texts to be translated.
    language_from : str, optional
        The text source language. The default is "auto".
    language_to : TYPE, optional
        The language target. It can be a list of languages.
        The default is ["en"].

    Raises
    ------
    Exception
        It returns an exception when the code of the response is not
        successful. You can find more details in the documentation below
        reagarding error codes and its description.
        https://learn.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-reference

    Returns
    -------
    response : list
        A json object.
    """
    # Add your key and endpoint
    adb_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    if adb_id == "6928573572659952":
        env = "d"
    if adb_id == "1108898350001842":
        env = "q"
    if adb_id == "2912385149360816":
        env = "p"
    if adb_id == "401517713111644":
        env = "p"

    endpoint = f"https://brewdat-bees-ghq-sales-cst-{env}-domain.cognitiveservices.azure.com/translator/text/v3.0"
    key = dbutils.secrets.get(
        scope=f"brewdatbeesghqsalesakv{env}",
        key=f"brewdat-bees-ghq-cognitive-translator-{env}",
    )
    # location, also known as region.
    # required if you're using a multi-service or regional (not global)
    # resource. It can be found in the Azure portal on the Keys and Endpoint
    # page.
    location = "westeurope"

    path = "/translate"
    constructed_url = endpoint + path

    if language_from == "auto":
        language_from = ""

    params = {"from": language_from, "to": language_to}

    headers = {
        "Ocp-Apim-Subscription-Key": key,
        # location required if you're using a multi-service or regional
        # (not global) resource.
        "Ocp-Apim-Subscription-Region": location,
        "Content-type": "application/json",
        "X-ClientTraceId": str(uuid.uuid4()),
    }

    # You can pass more than one object in body.
    body = [{"text": t} for t in text_list]

    request = requests.post(
        constructed_url, params=params, headers=headers, json=body, timeout=1000
    )
    try:
        response = request.json()

        if request.status_code != 200:
            raise Exception(f"Error when retrieving data: {response}")

        return response
    except:
        raise Exception(
            f"Error when retrieving data: status code: {request.status_code} reason: {request.reason}"
        )


def translate_column_dataframe(
    df, column, language_from="auto", language_to=["en"], enable_caching=True
) -> DataFrame:
    """

    Translate the desired column of a given dataframe to one or many languages.

    It is only supported one column at a time and the return is a new
    dataframe with original column and the translated column. You can pass a
    list of languages you wish to translate that the dataframe will contain
    the rows translated on each language. You can filter the language in the
    "to" field.

    Parameters
    ----------
    df : Spark Dataframe
        A Dataframe where the data to be translated is in.
    language_from : str, optional
        The text language source. The default is "auto".
    language_to : list
        A list of languages you can pass to translate. You can opt to translate
        as many languages you want. The default is ["en"].

    Raises
    ------
    Exception
        The dataframe cannot be empty.

    Returns
    -------
    Dataframe : Spark dataframe
        A new dataframe with column source and a column translated.
    """
    if df.count() == 0:
        raise Exception("Error: Dataframe cannot be empty")

    if enable_caching:
        df = df.cache()

    data = []

    df_translate = df.select(column).distinct()
    text_list = df_translate.rdd.flatMap(lambda x: x).collect()

    if len(text_list) > 1000:
        # split the data into chunks once we can only send 1000 elements per request
        data_chunked = [text_list[i : i + 1000] for i in range(0, len(text_list), 1000)]
        for item in data_chunked:
            data.append(azure_translator(item, language_from, language_to))
        # flatten the list's list into a single list
        data = [item for sublist in data for item in sublist]
    else:
        data = azure_translator(text_list, language_from, language_to)
    # add original text
    for i in range(len(data)):
        data[i][column] = text_list[i]

    df_translated = spark.read.json(sc.parallelize(data))
    df_translated = df_translated.selectExpr(column, "inline(translations)")

    df_final = df_translated.withColumnRenamed("text", f"{column}_translated")

    return df_final


# COMMAND ----------

"""
The type_mapping dictionary is used to map Azure Synapse Analytics data types to their corresponding PySpark data types.
It allows you to define the mapping between Synapse data types and the appropriate Spark data types for casting operations.
"""


def convert_to_spark_type(type: str):
    spark_type_dict = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "decimal": DecimalType(),
        "date": DateType(),
        "datetime2": TimestampType(),
        "timestamp": TimestampType(),
    }

    return spark_type_dict[type]


@curry
def rename_columns(column_map: Mapping[str, str], df: DataFrame) -> DataFrame:
    """
    Given a DataFrame and mapping of a subset of its column
    names to desired names, return the DataFrame that results
    from renaming those columns to their mapped values.

    :param column_map: Dictionary of column names mapped to their
      desired renaming
    :type column_map: dict
    :param df: DataFrame whose columns should be renamed
    :type df: DataFrame
    :return: DataFrame with columns renamed according to mapping
    :rtype: DataFrame
    """
    if column_map is None:
        column_map = {}
    return reduce(
        lambda acc, col: acc.withColumnRenamed(col, column_map[col]),
        column_map.keys() & set(df.columns),
        df,
    )


@curry
def cast_columns(
    type_map: Mapping[Union[Column, str], Union[DataType, str]], df: DataFrame
) -> DataFrame:
    """
    Given a mapping of DataFrame Columns (or their names) to DataTypes (or their names) and a DataFrame,
    return a DataFrame for which all columns that exist in both the mapping and DataFrame are cast
    to the mapped type.

    :param type_map: Mapping between some representation of a DataFrame Column and the type to
    which it should be mapped
    :type type_map: Mapping[Union[Column, str], Union[DataType, str]]
    :param df: DataFrame whose columns should be cast
    :type df: DataFrame
    :return: DataFrame with columns cast according to the provided mapping
    :rtype: DataFrame
    """
    cols_to_cast = set(
        [col.name() if isinstance(col, Column) else col for col in type_map.keys()]
    ) & set(df.columns)

    def _cast(field_name: str, data_type: DataType, df: DataFrame):
        expression = df[field_name].cast(data_type)

        return df.withColumn(field_name, expression)

    return reduce(lambda acc, col: _cast(col, type_map[col], acc), cols_to_cast, df)


@curry
def apply_schema(schema: StructType, df: DataFrame) -> DataFrame:
    """
    Apply a schema to an existing DataFrame by casting columns to
    specified types, adding missing columns, and dropping columns
    that do not exist in the schema.

    :param schema: Schema to apply
    :type schema: StructType
    :param df: DataFrame to apply schema to
    :type df: DataFrame
    :return: DataFrame with schema applied
    :rtype: DataFrame
    """

    def _add_if_missing(field: StructField, df: DataFrame):
        if field.name not in df.columns:
            return df.withColumn(field.name, F.lit(None).cast(field.dataType))
        return df

    return reduce(
        lambda acc, field: _add_if_missing(field, acc),
        schema.fields,
        df.transform(
            cast_columns(
                {f.name: f.dataType for f in schema.fields},
            )
        ),
    ).select(*schema.fieldNames())


@curry
def sort_columns(df: DataFrame, column_positions: dict):
    """
    Sorts a DataFrame based on a dictionary that contains the relationship between columns and their respective positions.

    :param df: DataFrame to be sorted
    :param column_positions: Dictionary with the relationship between columns and their positions
    :param ascending: Indicates whether the sorting should be ascending (True) or descending (False)
    :return: Sorted DataFrame
    """
    sorted_columns = sorted(column_positions.items(), key=lambda item: item[1])
    sorted_columns = [column for column, index in sorted_columns]

    return df.select(*sorted_columns)


@curry
def contract_json_to_structType(schema: list):
    """Converts a Spark DataFrame into a StructType schema.

    Args:
        df (DataFrame): The Spark DataFrame containing schema information.

    Returns:
        StructType: The resulting StructType that represents the DataFrame schema.
    """
    schema_fields = []

    for column in schema:
        column_name = column["column_name"]
        column_type = column["type"]

        if column_type == "decimal":
            column_precision = column["precision"]
            p1, p2 = map(int, column_precision.strip("()").split(","))

            field = StructField(column_name, DecimalType(p1, p2), nullable=True)
        else:
            field = StructField(
                column_name, convert_to_spark_type(column_type), nullable=True
            )

        schema_fields.append(field)

    struct_type = StructType(schema_fields)
    return struct_type


@curry
def column_initcap_value(column_nm: str, df: DataFrame) -> DataFrame:
    """Convert column values to Initcap values.
    e.g brahma -> Brahma

    Args:
        df (DataFrame): The Spark dataframe we'll apply the transformation
        column_nm (str): The Dataframe column we'll apply the value transformation

    Returns:
        DataFrame: The resulting DataFrame
    """
    return df.withColumn(column_nm, F.initcap(F.col(column_nm)))


@curry
def column_upper_value(column_nm: str, df: DataFrame) -> DataFrame:
    """Convert column values to upper case.

    Args:
        df (DataFrame): The Spark dataframe we'll apply the transformation
        column_nm (str): The Dataframe column we'll apply the value transformation

    Returns:
        DataFrame: The resulting DataFrame
    """
    return df.withColumn(column_nm, F.upper(F.col(column_nm)))


@curry
def normalization_values(dict: dict, df: DataFrame) -> DataFrame:
    """Normalize columns values based on dict.

    Args:
        dict (dict): The dict with all the rules in backgroud of each field
        df (DataFrame): The Spark dataframe we'll apply the transformation

    Returns:
        DataFrame: The resulting DataFrame
    """

    default_dict = dict.get("default")
    custom_dict = dict.get("custom")

    def _normalization_default(column: str, item: str, value: str, df: DataFrame):
        return df.withColumn(
            column, F.when(F.col(column) == item, value).otherwise(F.col(column))
        )

    def _normalization_custom(
        column: str, column_ref: str, item: str, value: str, df: DataFrame
    ):
        return df.withColumn(
            column, F.when(F.col(column_ref) == item, value).otherwise(F.col(column))
        )

    if default_dict != None:
        for column in default_dict.keys():
            for item in default_dict[column].keys():
                value = default_dict[column][item]
                df = _normalization_default(column, item, value, df)
    if custom_dict != None:
        for column in custom_dict.keys():
            for item in custom_dict[column].keys():
                value = custom_dict[column][item]
                column_ref, item = map(str, item.split(","))
                df = _normalization_custom(column, column_ref, item, value, df)

    return df
