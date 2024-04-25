# Databricks notebook source
# MAGIC %run ./validation_functions

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./settings

# COMMAND ----------

from pyspark.sql import DataFrame
import typing

# COMMAND ----------


def write_into_synapse_using_copy(settings: dict, dataframe: DataFrame):
    """This code defines a function called `write_into_synapse_using_copy` that writes data from a DataFrame into a table in Synapse Analytics.
       The function takes in several parameters including project name, database name, table name, target schema name, sql dw endpoint, load behavior, and the DataFrame to be written.
       The code first does some preliminary checks such as whether the SQLDW endpoint and project name are valid, and whether the polybase folder exists for the given project. If any of these checks fail, an exception is raised.
       If the load behavior is set to "overwrite_table", pre and post actions are defined to truncate the target table and update statistics after writing the new data. The function then prints out several variables before writing the data to Synapse using the `com.databricks.spark.sqldw` connector.
       If any errors occur during this process, the function will exit with the last exception using a utility function called `common_utils.exit_with_last_exception()`.

    Args:
        settings (dict): write settings

    """
    try:
        # Temporary path
        storage_account_suffix = (
            f"brewdatblobsagb{ environment.lower() }.dfs.core.windows.net"
        )

        # Set blob and synapse dwh configuration - The blob and user are fixed and not configurable by the project
        Utils.set_write_synapse_access_configuration()

        # Create data class settings
        write_settings = WriteSynapseSettings(**settings)

        # print our settings before write at synapse
        print(write_settings)

        # Checking if we have data to write at synapse
        if dataframe.count() > 0:

            tempDir = f"abfss://bees-plz@{ storage_account_suffix }/workspace/{ write_settings.project }/working/polybase"

            if path_exists(tempDir):
                # Setting pre and post behaviors
                if write_settings.load_type == "overwrite":

                    pre_actions = f"TRUNCATE TABLE {write_settings.getFullTableName}"
                    post_actions = (
                        f"UPDATE STATISTICS {write_settings.getFullTableName}"
                    )

                    # Write the data at synapse
                    (
                        dataframe.write.format("com.databricks.spark.sqldw")
                        .mode("append")
                        .option("url", write_settings.getSqlDWConnectionString)
                        .option("enableServicePrincipalAuth", True)
                        .option("useAzureMSI", True)
                        .option("tempDir", tempDir)
                        .option("preActions", pre_actions)
                        .option("dbTable", write_settings.getFullTableName)
                        .option("postActions", post_actions)
                        .save()
                    )
                elif (
                    write_settings.load_type == "incremental"
                    and write_settings.pre_actions != None
                    and write_settings.post_actions != None
                ):
                    (
                        dataframe.write.format("com.databricks.spark.sqldw")
                        .mode("append")
                        .option("url", write_settings.getSqlDWConnectionString)
                        .option("enableServicePrincipalAuth", True)
                        .option("useAzureMSI", True)
                        .option("tempDir", tempDir)
                        .option("preActions", write_settings.pre_actions)
                        .option("dbTable", write_settings.getFullTableName)
                        .option("postActions", write_settings.post_actions)
                        .save()
                    )
                elif write_settings.load_type == "incremental" and (
                    write_settings.pre_actions == None
                    or write_settings.post_actions == None
                ):
                    raise Exception(
                        f"Invalid write_settings.load_type: {write_settings.load_type}, the fields pre_action and post_action must be filled in."
                    )
            else:
                raise Exception(
                    f"The polybase folder does not exists. Please contact you data architecture team. {tempDir}"
                )
    except:
        common_utils.exit_with_last_exception()
