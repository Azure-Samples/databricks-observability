output "connection_string" {
  value     = azurerm_application_insights.default.connection_string
  sensitive = true
}
