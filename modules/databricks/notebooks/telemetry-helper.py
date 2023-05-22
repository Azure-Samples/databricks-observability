# Databricks notebook source
"""Utilities to configure the OpenTelemetry SDK to export telemetry to
Application Insights. Use the following APIs to _raise_ telemetry:

- Traces: `opentelemetry.trace`
- Metrics: `opentelemetry.metrics`
- Logs: `logging`
"""
from typing import Any, NamedTuple

_initialized = False  # Stores whether Application Insights was already configured.

class ApplicationInsightsConfiguration(NamedTuple):
    import logging

    """Holds configuration for configuring the OpenTelemetry exporters for Azure Monitor.
    :connection_string: connection string of the Application Insights instance. If not specified, uses APPLICATIONINSIGHTS_CONNECTION_STRING environment variable.
    :service_name: name of the service
    :service_version: version of the service
    :logging_level: logging level, defined in terms of the standard Python logging facility
    :tracing_export_interval_ms: how frequently to export traces
    :logging_export_interval_ms: how frequently to export logs

    The metrics export interval can only be configured through setting the
    OTEL_METRIC_EXPORT_INTERVAL environment variable.
    """

    service_name: str
    service_version: str
    connection_string: str = None
    logging_level: int = logging.INFO
    tracing_export_interval_ms: int = 15000
    logging_export_interval_ms: int = 15000

    def configure(self):
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry.sdk.resources import Resource, ResourceAttributes
        from uuid import uuid4

        # Service name has a default SDK environment variable. For consistency
        # we require the parameter instead of the environment variable.
        # https://opentelemetry.io/docs/concepts/sdk-configuration/general-sdk-configuration/
        assert self.service_name, "service_name must be defined"
        assert self.service_version, "service_version must be defined"

        # Configure Azure Monitor exporters for the OpenTelemetry SDK.
        global _initialized
        if not _initialized:
            configure_azure_monitor(
                resource=Resource.create({
                    ResourceAttributes.SERVICE_NAME: self.service_name,
                    ResourceAttributes.SERVICE_INSTANCE_ID: str(uuid4()),
                    ResourceAttributes.SERVICE_VERSION: self.service_version,
                }),
                tracing_export_interval_ms=self.tracing_export_interval_ms,
                logging_export_interval_ms=self.logging_export_interval_ms,
                logging_level=self.logging_level
            )

            _initialized = True


def default_notebook_configuration() -> ApplicationInsightsConfiguration:
    """Gets the default Application Insights configuration based on convention for all Databricks
    notebooks in our solution. Meaning:

    - The `ApplicationInsightsConnectionString` is set in the `APPLICATIONINSIGHTS_CONNECTION_STRING`
      environment variable."""

    return ApplicationInsightsConfiguration(
        service_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        service_version="1.0")

# COMMAND ----------

trace_context_carrier = None
try:
    trace_context_carrier = dbutils.widgets.get("_opentelemetry_trace_context")
except:
    pass

if trace_context_carrier:
    from opentelemetry import context
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    import json
    ctx = TraceContextTextMapPropagator().extract(carrier=json.loads(trace_context_carrier))
    token = context.attach(ctx)
del(trace_context_carrier)

# COMMAND ----------

import logging
from opentelemetry import trace, metrics

app_insights_config = default_notebook_configuration()
app_insights_config.configure()

notebook_name = app_insights_config.service_name
del(app_insights_config)

tracer = trace.get_tracer(notebook_name)

logger = logging.getLogger(notebook_name)
logger.setLevel(logging.DEBUG)

meter = metrics.get_meter(notebook_name)

# COMMAND ----------

def run_with_telemetry(path: str, timeout_seconds: int, arguments: Any = None) -> str:
    """This method runs a notebook and returns its exit value.

    It invokes dbutils.notebook.run() with the provided parameters.

    In addition, it wraps the invocation in telemetry spans for the current
    and invoked notebook, and passes the telemetry context to the notebook.
    Calling "%run ./telemetry-helper" causes the telemetry context to be
    restored in the invoked notebook, attaching spans and traces to the
    notebook span.
    """

    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    import json

    if arguments is None:
        arguments = dict()

    with tracer.start_as_current_span(notebook_name):
        with tracer.start_as_current_span(path):
            carrier = {}
            # Write the current context into the carrier.
            TraceContextTextMapPropagator().inject(carrier)
            token = json.dumps(carrier)
            arguments["_opentelemetry_trace_context"] = token
            dbutils.notebook.run(path, timeout_seconds, arguments)
