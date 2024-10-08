import logging
import os
import time
from typing import Dict, List

import pkg_resources
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
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

        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(attributes={SERVICE_NAME: tracing_service_name})
            )
        )
        FlaskInstrumentor().instrument(
            request_hook=request_hook, response_hook=response_hook
        )

    if tracing_export == "otlp":
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter())
        )
    elif tracing_export == "file":
        timestamp_millis = int(time.time() * 1000)
        file_name = f"otel_spans_integration_testing_{timestamp_millis}.ndjson"
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
        processor = SimpleSpanProcessor(FileSpanExporter(file_path))
        trace.get_tracer_provider().add_span_processor(processor)
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


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

    try:
        my_version = pkg_resources.get_distribution("schematicpy").version
        span.set_attribute(key="schematic.version", value=my_version)
    except Exception:
        logger.exception("Failed to set package version info in span")


def response_hook(span: Span, status: str, response_headers: List) -> None:
    """Nothing is implemented here yet, but it follows the same pattern as the
    request hook."""
    pass


set_up_tracing()
