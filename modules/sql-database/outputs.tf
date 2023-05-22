output "jdbc_connection_string" {
  description = "JDBC Connection string for the Azure SQL Database."
  value       = "jdbc:sqlserver://${azurerm_mssql_server.sql-server.fully_qualified_domain_name}:1433;database=${azurerm_mssql_database.sql-db.name}"
}

output "username" {
  value = local.username
}

output "password_secret_name" {
  value = azurerm_key_vault_secret.db_pw.name
}