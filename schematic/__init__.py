import gzip
import logging
import os
import time
import zlib
from io import BytesIO
from time import sleep
from typing import Dict, List

import pkg_resources
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    Span,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from synapseclient import Synapse
from werkzeug import Request

from schematic.configuration.configuration import CONFIG
from schematic.loader import LOADER
from schematic_api.api.security_controller import info_from_bearer_auth

Synapse.allow_client_caching(False)
logger = logging.getLogger(__name__)


# borrowed from: https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/tests/integration/conftest.py
class FileSpanExporter(ConsoleSpanExporter):
    """Create an exporter for OTEL data to a file."""

    def __init__(self, file_path: str) -> None:
        """Init with a path."""
        self.file_path = file_path

    def export(self, spans: List[Span]) -> None:
        """Export the spans to the file."""
        with open(self.file_path, "a", encoding="utf-8") as f:
            for span in spans:
                span_json_one_line = span.to_json().replace("\n", "") + "\n"
                f.write(span_json_one_line)


def set_up_tracing() -> None:
    """Set up tracing for the API."""
    tracing_export = os.environ.get("TRACING_EXPORT_FORMAT", None)
    if tracing_export is not None and tracing_export:
        Synapse.enable_open_telemetry(True)
        tracing_service_name = os.environ.get("TRACING_SERVICE_NAME", "schematic-api")
        deployment_environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", "")
        package_version = pkg_resources.get_distribution("schematicpy").version
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(
                    attributes={
                        SERVICE_NAME: tracing_service_name,
                        SERVICE_VERSION: package_version,
                        DEPLOYMENT_ENVIRONMENT: deployment_environment,
                    }
                )
            )
        )
        FlaskInstrumentor().instrument(
            request_hook=request_hook, response_hook=response_hook
        )

    if tracing_export == "otlp":
        exporter = OTLPSpanExporter()
        import types

        exporter._export = types.MethodType(trace_export_replacement, exporter)
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))
    elif tracing_export == "file":
        timestamp_millis = int(time.time() * 1000)
        file_name = f"otel_spans_integration_testing_{timestamp_millis}.ndjson"
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
        processor = SimpleSpanProcessor(FileSpanExporter(file_path))
        trace.get_tracer_provider().add_span_processor(processor)
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


def set_up_logging() -> None:
    """Set up logging to export to OTLP."""
    logging_export = os.environ.get("LOGGING_EXPORT_FORMAT", None)
    logging_service_name = os.environ.get("LOGGING_SERVICE_NAME", "schematic-api")
    deployment_environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", "")
    if logging_export == "otlp":
        resource = Resource.create(
            {
                SERVICE_NAME: logging_service_name,
                DEPLOYMENT_ENVIRONMENT: deployment_environment,
            }
        )

        # logger_provider = LoggerProvider(resource=resource)
        # set_logger_provider(logger_provider=logger_provider)

        # exporter = OTLPLogExporter()
        # # exporter._certificate_file = False
        # logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        # handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        # logging.getLogger().addHandler(handler)


def request_hook(span: Span, environ: Dict) -> None:
    """
    Request hook for the flask server to handle setting attributes in the span. If
    anything here happens to fail we do not want to stop the request from being
    processed so we catch all exceptions and log them.

    Args:
        span: The span object to set attributes in
        environ: The environment variables from the request
    """
    if not span or not span.is_recording():
        return
    try:
        if auth_header := environ.get("HTTP_AUTHORIZATION", None):
            split_headers = auth_header.split(" ")
            if len(split_headers) > 1:
                token = auth_header.split(" ")[1]
                user_info = info_from_bearer_auth(token)
                if user_info:
                    span.set_attribute("user.id", user_info.get("sub"))
    except Exception:
        logger.exception("Failed to set user info in span")

    try:
        if (request := environ.get("werkzeug.request", None)) and isinstance(
            request, Request
        ):
            for arg in request.args:
                span.set_attribute(key=f"schematic.{arg}", value=request.args[arg])
    except Exception:
        logger.exception("Failed to set request info in span")


def response_hook(span: Span, status: str, response_headers: List) -> None:
    """Nothing is implemented here yet, but it follows the same pattern as the
    request hook."""
    pass


# This is a temporary hack to get around the fact that I am currently using self-signed SSL certs.
# I have an IT ticket to get a domain to use for this purpose so I can request a proper SSL cert from LetsEncrypt.
def trace_export_replacement(self, serialized_data: bytes):
    data = serialized_data
    if self._compression == Compression.Gzip:
        gzip_data = BytesIO()
        with gzip.GzipFile(fileobj=gzip_data, mode="w") as gzip_stream:
            gzip_stream.write(serialized_data)
        data = gzip_data.getvalue()
    elif self._compression == Compression.Deflate:
        data = zlib.compress(serialized_data)

    return self._session.post(
        url=self._endpoint,
        data=data,
        verify=False,
        timeout=self._timeout,
        cert=self._client_cert,
    )


set_up_tracing()
set_up_logging()
