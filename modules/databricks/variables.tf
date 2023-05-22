variable "name_part1" {
}

variable "name_part2" {
}
variable "location" {
}
variable "resource_group_name" {
}

variable "key_vault_id" {
}

variable "metastore_username" {
}

variable "metastore_password_secret_name" {
}

variable "metastore_connection_string" {
}

variable "app_insights_connection_string" {
  sensitive = true
}
