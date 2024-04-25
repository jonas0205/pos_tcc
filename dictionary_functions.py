# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class CommonData:
    """Class that can automate work related to the Commom data
    Methods
    -------
    get_dictionary -> dataframe
      Return the dictionary according to the dataset and version
    get_columns -> list
      Return the columns name from the dictionary according to the dataset and version
    get_columns_and_types -> list[tuples]
      Return the columns name and type from the dictionary according to the dataset and version
    get_query -> str
      Build a query following the columns name and type that were defined in the dictionary
    """

    DICTIONARY_FOLDER = f"{LAKEHOUSE_CONFIG_ROOT}/quality/data_dictionary/"

    # Dictionary_folder_variable
    def __init__(
        self, dataset: str, version: str = None, source_table_name: str = None
    ) -> None:
        """Instantiate the object
        Parameters:
        -----------
        dataset:str
          Define the entity
        version:str
          Define the version of the Commom Data
        dataframe:dataframe
          Dataframe used to define the version of Commom Data
        """
        try:
            self.dataset = dataset.lower()
            self._set_version(version, source_table_name)

        except:
            common_utils.exit_with_last_exception()

    def _set_version(self, version: str, source_table_name: str) -> None:
        """Define the version of the Dataset.
            Can be defined automatic by looking at the data/dataframe or
            the user can pass a version that they want.
        Parameters:
        -----------
        version:str
          Define the version of the Commom Data
        dataframe:dataframe
          Dataframe used to define the version of Commom Data
        """
        if version:
            self.version = version.lower()
        else:
            dataframe = spark.read.table(source_table_name)

            if dataframe.count() == 0:
                raise Exception("Empty Dataframe. Please verify the source table")
            else:
                self.version = str(
                    dataframe.select("extract_version_cd")
                    .distinct()
                    .orderBy(F.desc("extract_day"))
                    .head()[0]
                ).lower()

    def get_dictionary(self) -> DataFrame:
        """Return the Diciotnary based on the dataset and the version."""
        try:
            where_condiction = f"""LOWER(extract_version_cd) = '{self.version}' 
                               and LOWER(dataset)  = '{self.dataset}'"""
            dictionary = (
                spark.read.format("csv")
                .option("header", True)
                .load(CommonData.DICTIONARY_FOLDER)
                .where(where_condiction)
            )
            return dictionary

        except:
            common_utils.exit_with_last_exception()

    def get_columns(self, priority: int = None) -> list:
        """Return the columns name from the dictionary according to the dataset and version
        Parameters:
        -----------
        priority:int
          The user can define if they need all the columns or only the coluns with priority 1 for exemple
        """
        dictionary = self.get_dictionary()

        try:
            # If the user has semd a priority we need to filter only the rows that matchs the priority
            if priority:
                list_columns_from_dictionary = (
                    dictionary.filter(f"priority = {priority}")
                    .select("column_name")
                    .collect()
                )

            else:
                list_columns_from_dictionary = dictionary.select(
                    "column_name"
                ).collect()

            return [row.column_name for row in list_columns_from_dictionary]

        except:
            common_utils.exit_with_last_exception()

    def get_columns_and_types(self, priority=None) -> list:
        """Return the columns name and type from the dictionary according to the dataset and version
        Parameters:
        -----------
        priority:int
          The user can define if they need all the columns or only the coluns with priority 1 for exemple
        """
        dictionary = self.get_dictionary()

        try:
            # If the user has semd a priority we need to filter only the rows that matchs the priority
            if priority:
                list_columns_types_from_dictionary = (
                    dictionary.filter(f"priority = {priority}")
                    .select("column_name", "column_type")
                    .collect()
                )

            else:
                list_columns_types_from_dictionary = dictionary.select(
                    "column_name", "column_type"
                ).collect()

            return [
                (row.column_name, row.column_type)
                for row in list_columns_types_from_dictionary
            ]

        except:
            common_utils.exit_with_last_exception()

    def get_priority(self, project: str) -> list:
        dictionary = self.get_dictionary()
        try:
            list_columns_types_from_dictionary = (
                dictionary.select("column_name", project)
                .withColumnRenamed(project, "priority")
                .collect()
            )
            return [
                (row.column_name, row.priority)
                for row in list_columns_types_from_dictionary
            ]

        except:
            common_utils.exit_with_last_exception()

    def get_index(self) -> list:
        dictionary = self.get_dictionary()
        try:
            list_columns_types_from_dictionary = (
                dictionary.select("column_name", "index")
                .withColumnRenamed("index", "index_column")
                .collect()
            )
            return [
                (row.column_name, row.index_column)
                for row in list_columns_types_from_dictionary
            ]

        except:
            common_utils.exit_with_last_exception()

    def difference_between_dictionary_dataframe(
        self, dictionary_columns: list, source_table_columns: list
    ) -> list:
        try:
            return list(set(dictionary_columns) - set(source_table_columns))

        except:
            common_utils.exit_with_last_exception()

    def get_query(
        self, source_table_name: str, custom_transformation: dict = None
    ) -> str:
        """Build a query following the columns name and type that were defined in the dictionary
        Parameters:
        -----------
        source_table_name:str
          Name of the Table in the bronze layer
        custom_transformation:dict
          Transformation customize that may be necessary in the select statement. Ex TO_DATE() transformation
        """
        try:
            column_not_in_df = self.difference_between_dictionary_dataframe(
                dictionary_columns=self.get_columns(),
                source_table_columns=(spark.read.table(source_table_name).columns),
            )

            ##Print dataset version
            print(
                f"DATASET: {self.dataset.upper()} \t VERSION: {self.version.upper()}\n"
            )

            ## Compar columns in the dicitonary with the coluns in the source table
            print(
                f"COLUMNS IN THE DICT AND NOT IN THE SOURCE TABLE => {column_not_in_df}"
            )

            dictionary_columns_and_types = self.get_columns_and_types()
            column_query = ""
            for column_name, column_type in dictionary_columns_and_types:

                # Handle columns that exist in the dictionary but isn't available in the source table
                # We only cast the column and put a null value in it.
                if column_name in column_not_in_df:
                    column_query += f"CAST(NULL AS {column_type}) AS {column_name},\n"

                # Handle columns that exist in both dictionary and the source table
                else:
                    # if string it's not necessary to cast. All the columns in bronze are string by default
                    if column_type.lower() == "string":
                        column_query += f"{column_name} AS {column_name},\n"

                    # If its necesary some custom transformation in the column you can pass a dictionary with the transformation
                    elif custom_transformation and column_name in custom_transformation:
                        column_query += (
                            f"{custom_transformation[column_name]} AS {column_name},\n"
                        )

                    # If we only need to cast the data based on the dictionary
                    else:
                        column_query += (
                            f"CAST({column_name} AS {column_type}) AS {column_name},\n"
                        )

            header_query = "SELECT "
            fotter_query = f"""extract_yr, \nextract_mth,\nextract_day
                      FROM {source_table_name}
                      WHERE 1 = 1
                      AND extract_yr BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyy')
                      AND DATE_FORMAT('{watermark_end_datetime}', 'yyyy')
                      AND extract_mth BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyyMM')
                      AND DATE_FORMAT('{watermark_end_datetime}', 'yyyyMM')
                      AND extract_day BETWEEN DATE_FORMAT('{watermark_start_datetime}', 'yyyyMMdd')
                      AND DATE_FORMAT('{watermark_end_datetime}', 'yyyyMMdd')
                      """
            final_query = header_query + column_query + fotter_query

            print("\n--QUERY_GENERATED--\n")
            print(final_query)

            return final_query

        except:
            common_utils.exit_with_last_exception()

    def get_primary_key(self) -> list:
        """Return the primary key of the dictionary dataset"""
        dictionary = self.get_dictionary()

        try:
            pk_list = dictionary.filter("upper(key) = 'PK'").collect()

            return [row.column_name for row in pk_list]

        except:
            common_utils.exit_with_last_exception()

    def get_foreign_key(self):
        """Return the foreign key of the dictionary dataset"""
        dictionary = self.get_dictionary()

        try:
            fk_list = dictionary.filter("upper(key) = 'FK'").collect()

            return [row.column_name for row in fk_list]

        except:
            common_utils.exit_with_last_exception()

    def build_dictionary_object(self):
        """Return the dictionary related to a dataset in a object template
        that sometimes may be friendly to use
        """
        return {
            "table_name": self.dataset,
            "table_primary_key": self.get_primary_key(),
            "table_foreign_key": self.get_foreign_key(),
            "columns_info": [self.get_columns_and_types()],
        }
