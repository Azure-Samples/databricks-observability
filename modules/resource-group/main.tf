resource "azurerm_resource_group" "rg" {
  name     = format("rg-%s", var.name_part1)
  location = var.location
}