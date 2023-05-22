terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

resource "databricks_notebook" "main" {
  source = "${path.module}/../notebooks/${var.notebook_name}.py"
  path   = "/Shared/${var.notebook_name}"
}

resource "databricks_job" "main" {
  name = var.job_name

  task {
    task_key = "a"

    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = databricks_notebook.main.path
    }
  }

  schedule {
    quartz_cron_expression = "0 * * * * ?" # every minute
    timezone_id            = "UTC"
  }
}