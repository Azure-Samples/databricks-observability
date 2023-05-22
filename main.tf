terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.53.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "=1.14.3"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "databricks" {
  azure_workspace_resource_id = module.databricks.adb_id
}

resource "random_id" "storage_account" {
  byte_length = 8
}

locals {
  name_part1 = var.base_resource_name
  name_part2 = lower(random_id.storage_account.hex)
}

module "rg" {
  source     = "./modules/resource-group"
  name_part1 = local.name_part1
  location   = var.location
}

module "app-insights" {
  source              = "./modules/app-insights"
  resource_group_name = module.rg.name
  name_part1          = local.name_part1
  name_part2          = local.name_part2
  location            = var.location
}

module "databricks" {
  source                         = "./modules/databricks"
  resource_group_name            = module.rg.name
  name_part1                     = local.name_part1
  name_part2                     = local.name_part2
  location                       = var.location
  key_vault_id                   = module.keyvault.kv_id
  metastore_connection_string    = module.sql-database.jdbc_connection_string
  metastore_username             = module.sql-database.username
  metastore_password_secret_name = module.sql-database.password_secret_name
  app_insights_connection_string = module.app-insights.connection_string
}

module "keyvault" {
  source              = "./modules/keyvault"
  resource_group_name = module.rg.name
  name_part1          = local.name_part1
  name_part2          = local.name_part2
  location            = var.location
}


module "sql-database" {
  source              = "./modules/sql-database"
  resource_group_name = module.rg.name
  name_part1          = local.name_part1
  name_part2          = local.name_part2
  location            = var.location
  key_vault_id        = module.keyvault.kv_id
}
