# Databricks notebook source
# MAGIC %run ./validation_functions

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./settings

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------


class SynapseAnalyticsReader:
    """This class is responsable to handle any necessity of read actions for any Synapse Analytics endpoint."""

    def read_synapse_analytics_table(settings: dict) -> DataFrame:
        """This code is responsable to encapsulate the read action for any synapse analytics when we want to read a table.

        Args:
            settings (dict): read settings

        """
        try:
            # Temporary path
            storage_account_suffix = (
                f"brewdatblobsagb{ environment.lower() }.dfs.core.windows.net"
            )

            # Set blob and synapse dwh configuration - The blob and user are fixed and not configurable by the project
            Utils.set_write_synapse_access_configuration()

            # Create data class settings
            read_synapse_settings = ReadSynapseAnalyticsSettings(**settings)

            # creating tempDir
            tempDir = f"abfss://bees-plz@{ storage_account_suffix }/workspace/{ read_synapse_settings.project.lower() }/working/polybase"

            if path_exists(tempDir):

                print(read_synapse_settings)

                # Checking if the read strategy will use a query or a source table.
                if read_synapse_settings.query:

                    # return the dataframe with data
                    return (
                        spark.read.format("com.databricks.spark.sqldw")
                        .option(
                            "url",
                            read_synapse_settings.synapse_analytics_conn_string.lower(),
                        )
                        .option("tempDir", tempDir)
                        .option(
                            "enableServicePrincipalAuth", True
                        )  # we should have it explicit
                        .option("useAzureMSI", True)  # we should have it explicit
                        .option("query", read_synapse_settings.query)
                        .load()
                    )

                elif read_synapse_settings.full_table_name:

                    # return the dataframe with data
                    return (
                        spark.read.format("com.databricks.spark.sqldw")
                        .option(
                            "url",
                            read_synapse_settings.synapse_analytics_conn_string.lower(),
                        )
                        .option("tempDir", tempDir)
                        .option(
                            "enableServicePrincipalAuth", True
                        )  # we should have it explicit
                        .option("useAzureMSI", True)  # we should have it explicit
                        .option(
                            "dbTable", read_synapse_settings.full_table_name.lower()
                        )
                        .load()
                    )

            else:
                raise Exception(
                    f"The polybase folder does not exists. Please contact you data architecture team. {tempDir}"
                )
        except:
            common_utils.exit_with_last_exception()
