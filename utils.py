# Databricks notebook source
class Utils:
    def set_write_synapse_access_configuration():
        # Variable environment is created by bees fremework at the Main notebook
        # Config variables
        if environment.lower() == "prod":
            application_id = "54f17737-7163-4ca8-b6f0-bd80de359f2c"
            secret_name = "brewdat-spn-bees-ghq-sales-framework-rw-p"
        elif environment.lower() == "qa":
            application_id = "c6ddc2f4-ca82-40e7-84d8-486d1aaf39ef"
            secret_name = "brewdat-spn-bees-ghq-sales-framework-rw-q"
        elif environment.lower() == "dev":
            application_id = "3ffccef5-8b72-47a1-8511-ee08fa1a79aa"
            secret_name = "brewdat-spn-bees-ghq-sales-frameworks-rw-d"
        else:
            application_id = None
            secret_name = None

        spn_tenant_id = "cef04b19-7776-4a94-b89b-375c77a8f936"  # fixed
        storage_account_suffix = (
            f"brewdatblobsagb{ environment.lower() }.dfs.core.windows.net"
        )
        akv = f"brewdatakvgb{ environment.lower() }"
        secret_credential = dbutils.secrets.get(
            akv, secret_name
        )  # get the secret credentials

        # Defining the service principal credentials for the Azure storage account
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account_suffix}",
            "OAuth",
        )
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{storage_account_suffix}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{storage_account_suffix}",
            application_id,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{storage_account_suffix}",
            secret_credential,
        )
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account_suffix}",
            f"https://login.microsoftonline.com/{spn_tenant_id}/oauth2/token",
        )

        # Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
        spark.conf.set(
            "spark.databricks.sqldw.jdbc.service.principal.client.id",
            application_id,
        )
        spark.conf.set(
            "spark.databricks.sqldw.jdbc.service.principal.client.secret",
            secret_credential,
        )
