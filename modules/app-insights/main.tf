# Application Insights

resource "azurerm_log_analytics_workspace" "default" {
  name                = format("la-%s-%s", var.name_part1, var.name_part2)
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

resource "azurerm_application_insights" "default" {
  name                = format("appi-%s-%s", var.name_part1, var.name_part2)
  resource_group_name = var.resource_group_name
  location            = var.location
  workspace_id        = azurerm_log_analytics_workspace.default.id
  application_type    = "other"
}
