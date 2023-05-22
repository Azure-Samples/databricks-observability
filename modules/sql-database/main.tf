
resource "random_id" "username" {
  byte_length = 6
}

resource "random_password" "password" {
  length  = 10
  special = true
}

locals {
  username = random_id.username.hex
  password = random_password.password.result
}

resource "azurerm_key_vault_secret" "db_pw" {
  name         = "sql-server-password"
  value        = local.password
  key_vault_id = var.key_vault_id
}

resource "azurerm_mssql_server" "sql-server" {
  name                         = format("sqlserver-%s-%s", var.name_part1, var.name_part2)
  resource_group_name          = var.resource_group_name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = local.username
  administrator_login_password = local.password
}

resource "azurerm_mssql_firewall_rule" "azure-services" {
  name             = "Allow"
  server_id        = azurerm_mssql_server.sql-server.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_database" "sql-db" {
  name      = "metastoredb"
  server_id = azurerm_mssql_server.sql-server.id
  sku_name  = "Basic"
}
