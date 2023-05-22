terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on        = [azurerm_databricks_workspace.adb]
}

data "azurerm_key_vault_secret" "db-pw" {
  name         = var.metastore_password_secret_name
  key_vault_id = var.key_vault_id
}

resource "azurerm_storage_account" "dbstorage" {
  name                     = format("st%s%s", var.name_part1, var.name_part2)
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_container" "dbstorage" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.dbstorage.name
  container_access_type = "private"
}

resource "azurerm_databricks_workspace" "adb" {
  name                = format("adb-%s-%s", var.name_part1, var.name_part2)
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "standard"
}

resource "databricks_secret_scope" "default" {
  name                     = "terraform-demo-scope"
  initial_manage_principal = "users"
}

resource "databricks_secret" "metastore-password" {
  key          = "metastore-password"
  string_value = data.azurerm_key_vault_secret.db-pw.value
  scope        = databricks_secret_scope.default.name
}

resource "databricks_secret" "app-insights-connection-string" {
  key          = "app-insights-connection-string"
  string_value = var.app_insights_connection_string
  scope        = databricks_secret_scope.default.name
}

resource "databricks_secret" "storage-key" {
  key          = "storage-account-key-${azurerm_storage_account.dbstorage.name}"
  string_value = azurerm_storage_account.dbstorage.primary_access_key
  scope        = databricks_secret_scope.default.name
}

resource "databricks_dbfs_file" "log4j2-properties" {
  source = "${path.module}/log4j2.properties"
  path   = "/observability/log4j2.properties"
}

resource "databricks_dbfs_file" "agent" {
  source = "applicationinsights-agent.jar"
  path   = "/observability/applicationinsights-agent.jar"
}

# TODO: move this to a databricks_workspace_file after
# https://github.com/databricks/terraform-provider-databricks/pull/2266
# is merged. see:
# https://www.databricks.com/blog/securing-databricks-cluster-init-scripts
resource "databricks_dbfs_file" "init-observability" {
  source = "${path.module}/init-observability.sh"
  path   = "/observability/init-observability.sh"
}

resource "databricks_dbfs_file" "applicationinsights-driver-json" {
  source = "${path.module}/applicationinsights-driver.json"
  path   = "/observability/applicationinsights-driver.json"
}

resource "databricks_dbfs_file" "applicationinsights-executor-json" {
  source = "${path.module}/applicationinsights-executor.json"
  path   = "/observability/applicationinsights-executor.json"
}


locals {
  dbfs_prefix  = "/dbfs"
  java_options = "-javaagent:/tmp/applicationinsights-agent.jar -Dlog4j2.configurationFile=${local.dbfs_prefix}${databricks_dbfs_file.log4j2-properties.path}"
  # Not used, but defined in order to ensure the file is valid JSON.
  user_data1 = jsondecode(file("${path.module}/applicationinsights-driver.json"))
  user_data2 = jsondecode(file("${path.module}/applicationinsights-executor.json"))
}


resource "databricks_cluster" "default" {
  cluster_name            = "demo-cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 20
  autoscale {
    min_workers = 2
    max_workers = 3
  }
  spark_conf = {
    # Metastore config
    "spark.hadoop.javax.jdo.option.ConnectionDriverName" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    "spark.hadoop.javax.jdo.option.ConnectionURL" : var.metastore_connection_string
    "spark.hadoop.javax.jdo.option.ConnectionUserName" : var.metastore_username
    "spark.hadoop.javax.jdo.option.ConnectionPassword" : databricks_secret.metastore-password.config_reference
    "datanucleus.fixedDatastore" : false
    "datanucleus.autoCreateSchema" : true
    "hive.metastore.schema.verification" : false
    "datanucleus.schema.autoCreateTables" : true

    # Storage access
    "fs.azure.account.key.${azurerm_storage_account.dbstorage.name}.dfs.core.windows.net" : databricks_secret.storage-key.config_reference
    "storage_uri" : "abfss://${azurerm_storage_container.dbstorage.name}@${azurerm_storage_account.dbstorage.name}.dfs.core.windows.net"

    # Observability
    "spark.executor.extraJavaOptions" : "${local.java_options}"
    "spark.driver.extraJavaOptions" : "${local.java_options}"
    "spark.metrics.conf.*.sink.jmx.class" : "org.apache.spark.metrics.sink.JmxSink"
    "spark.metrics.namespace" : "spark"
    "spark.metrics.appStatusSource.enabled" : "true"
    "spark.sql.streaming.metricsEnabled" : "true"
  }

  spark_env_vars = {
    APPLICATIONINSIGHTS_CONNECTION_STRING = databricks_secret.app-insights-connection-string.config_reference

    # The time interval (in milliseconds) for exporting metrics. Default: 60000.
    OTEL_METRIC_EXPORT_INTERVAL = 5000
  }

  init_scripts {
    dbfs {
      destination = databricks_dbfs_file.init-observability.dbfs_path
    }
  }

  depends_on = [
    databricks_dbfs_file.log4j2-properties,
    databricks_dbfs_file.agent,
    databricks_dbfs_file.init-observability,
    databricks_dbfs_file.applicationinsights-driver-json,
    databricks_dbfs_file.applicationinsights-executor-json,
  ]

  cluster_log_conf {
    dbfs {
      destination = "dbfs:/cluster-logs"
    }
  }
}

resource "databricks_library" "opentelemetry" {
  cluster_id = databricks_cluster.default.id
  pypi {
    package = "azure-monitor-opentelemetry~=1.0.0b10"
  }
}

module "periodic-job" {
  source        = "./notebook-job"
  notebook_name = "sample-notebook"
  job_name      = "Periodic job"
  cluster_id    = databricks_cluster.default.id
}

module "streaming-job" {
  source        = "./notebook-job"
  notebook_name = "sample-streaming-notebook"
  job_name      = "Streaming job"
  cluster_id    = databricks_cluster.default.id
}

resource "databricks_notebook" "sample-telemetry-notebook" {
  source = "${path.module}/notebooks/sample-telemetry-notebook.py"
  path   = "/Shared/sample-telemetry-notebook"
}

resource "databricks_notebook" "telemetry-helper" {
  source = "${path.module}/notebooks/telemetry-helper.py"
  path   = "/Shared/telemetry-helper"
}

module "telemetry-job" {
  source        = "./notebook-job"
  notebook_name = "sample-telemetry-caller"
  job_name      = "Telemetry job"
  cluster_id    = databricks_cluster.default.id

  depends_on = [
    databricks_notebook.sample-telemetry-notebook,
    databricks_notebook.telemetry-helper,
  ]
}