==============
Open Telemetry
==============
OpenTelemetry is a powerful serviceability framework that helps us gain insights into the performance and behavior of the systems. It facilitates generation, collection, and management of telemetry data
such as traces and metrics to observability dashboards.

Configuration
-------------

To enable this feature edit ``tracing-enabled`` to true and update the ``tracing-backend-url`` in ``etc/telemetry-tracing.properties`` .

Configuration properties
------------------------

============================================ =====================================================================
Property Name                                Description
============================================ =====================================================================
``tracing-factory.name``                     Unique identifier for factory implementation to be registered
``tracing-enabled``                          Boolean value controlling if tracing is on or off
``tracing-backend-url``                      Points to backend for exporting telemetry data
``max-exporter-batch-size``                  Maximum number of spans that will be exported in one batch
``max-queue-size``                           Maximum number of spans that can be queued before being processed for export
``schedule-delay``                           Delay between batches of span export, controlling how frequently spans are exported
``exporter-timeout``                         How long the span exporter will wait for a batch of spans to be successfully sent before timing out
``trace-sampling-ratio``                     Double between 0.0 and 1.0 to specify the percentage of queries to be traced
``span-sampling``                            Boolean to enable/disable sampling. If enabled, spans are only generated for major operations
============================================ =====================================================================