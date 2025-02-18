import logging
import os
from typing import Dict, List

import requests
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import (
    LoggerProvider,
    LoggingHandler,
    LogRecordProcessor,
    LogRecord,
)
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import (
    TracerProvider,
    SpanProcessor,
    ReadableSpan,
    Span as SpanSdk,
)
from opentelemetry.trace import Span, SpanContext, get_current_span
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from synapseclient import Synapse, USER_AGENT
from werkzeug import Request

from schematic.configuration.configuration import CONFIG
from schematic.loader import LOADER
from schematic.version import __version__
from dotenv import load_dotenv
from schematic.utils.remove_sensitive_data_utils import (
    redact_string,
    redacted_sensitive_data_in_exception,
)

Synapse.allow_client_caching(False)
logger = logging.getLogger(__name__)

# Ensure environment variables are loaded
load_dotenv()

USER_AGENT_LIBRARY = {
    "User-Agent": USER_AGENT["User-Agent"] + f" schematic/{__version__}"
}

USER_AGENT_COMMAND_LINE = {
    "User-Agent": USER_AGENT["User-Agent"] + f" schematiccommandline/{__version__}"
}

USER_AGENT |= USER_AGENT_LIBRARY


class AttributePropagatingSpanProcessor(SpanProcessor):
    """A custom span processor that propagates specific attributes from the parent span
    to the child span when the child span is started.
    It also propagates the attributes to the parent span when the child span ends.

    Args:
        SpanProcessor (opentelemetry.sdk.trace.SpanProcessor): The base class that provides hooks for processing spans during their lifecycle
    """

    def __init__(self, attributes_to_propagate: List[str]) -> None:
        self.attributes_to_propagate = attributes_to_propagate

    def on_start(self, span: Span, parent_context: SpanContext) -> None:
        """Propagates attributes from the parent span to the child span.
        Arguments:
            span: The child span to which the attributes should be propagated.
            parent_context: The context of the parent span.
        Returns:
            None
        """
        parent_span = get_current_span()
        if parent_span is not None and parent_span.is_recording():
            for attribute in self.attributes_to_propagate:
                # Check if the attribute exists in the parent span's attributes
                attribute_val = parent_span.attributes.get(attribute)
                if attribute_val:
                    # Propagate the attribute to the current span
                    span.set_attribute(attribute, attribute_val)

    def on_end(self, span: Span) -> None:
        """Propagates attributes from the child span back to the parent span"""
        parent_span = get_current_span()
        if parent_span is not None and parent_span.is_recording():
            for attribute in self.attributes_to_propagate:
                child_val = span.attributes.get(attribute)
                parent_val = parent_span.attributes.get(attribute)
                if child_val and not parent_val:
                    # Propagate the attribute back to parent span
                    parent_span.set_attribute(attribute, child_val)

    def shutdown(self) -> None:
        """No-op method that does nothing when the span processor is shut down."""
        pass

    def force_flush(self, timeout_millis: int = 30000) -> None:
        """No-op method that does nothing when the span processor is forced to flush."""
        pass


class CustomFilter(LogRecordProcessor):
    """A custom log record processor that redacts sensitive data from log messages before they are exported."""

    def __init__(self, exporter):
        self._exporter = exporter
        self._shutdown = False

    def emit(self, log_record: LogRecord) -> None:
        """Modify log traces before they are exported."""
        # Redact sensitive data in the log message (body)
        if log_record.log_record.body and "googleapis" in log_record.log_record.body:
            log_record.log_record.body = redact_string(log_record.log_record.body)

    def force_flush(self, timeout_millis=30000) -> bool:
        """Flush any pending log records (if needed)."""
        return self._exporter.force_flush(timeout_millis)

    def shutdown(self) -> None:
        """Clean up resources."""
        self._shutdown = True
        self._exporter.shutdown()


def create_telemetry_session() -> requests.Session:
    """
    Create a requests session with authorization enabled if environment variables are set.
    If no environment variables are set, the session will be created without authorization.

    Returns:
        requests.Session: A session object with authorization enabled if environment
            variables are set. If no environment variables are set, the session will be
            created without authorization. If no telemetry export format is set, None
            will be returned.
    """
    tracing_export = os.environ.get("TRACING_EXPORT_FORMAT", None)
    logging_export = os.environ.get("LOGGING_EXPORT_FORMAT", None)
    if not (tracing_export or logging_export):
        return None

    session = requests.Session()
    static_otlp_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", None)
    if static_otlp_headers:
        logger.info(
            "Using static OTLP headers set in environment variable `OTEL_EXPORTER_OTLP_HEADERS`."
        )
        return session

    logger.warning(
        "No environment variable `OTEL_EXPORTER_OTLP_HEADERS` provided for telemetry exporter. Telemetry data will be sent without any headers."
    )
    return session


def set_up_tracing(session: requests.Session) -> None:
    """Set up tracing for the API.
    Args:
        session: requests.Session object to use for exporting telemetry data. If
            the exporter is set to OTLP, this session will be used to send the data.
            If the exporter is set to file, this session will be ignored.
    """
    tracing_export = os.environ.get("TRACING_EXPORT_FORMAT", None)
    if tracing_export is not None and tracing_export:
        Synapse.enable_open_telemetry(True)
        tracing_service_name = os.environ.get("TRACING_SERVICE_NAME", "schematic-api")
        deployment_environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", "")
        service_instance_id = os.environ.get("SERVICE_INSTANCE_ID", "")
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(
                    attributes={
                        SERVICE_INSTANCE_ID: service_instance_id,
                        SERVICE_NAME: tracing_service_name,
                        SERVICE_VERSION: __version__,
                        DEPLOYMENT_ENVIRONMENT: deployment_environment,
                    }
                )
            )
        )
        FlaskInstrumentor().instrument(
            request_hook=request_hook, response_hook=response_hook
        )

    if tracing_export == "otlp":
        # Add the custom AttributePropagatingSpanProcessor to propagate attributes
        attribute_propagator = AttributePropagatingSpanProcessor(["user.id"])
        trace.get_tracer_provider().add_span_processor(attribute_propagator)
        exporter = OTLPSpanExporter(session=session)
        # Overwrite the _readable_span method to redact sensitive data
        SpanSdk._readable_span = _readable_span_alternate
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


original_function_readable_span = SpanSdk._readable_span


def _readable_span_alternate(self: SpanSdk) -> ReadableSpan:
    """Alternative function to the readable span. This function redacts sensitive data from the span attributes and events.

    Args:
        self (SpanSdk): _readable_span method of the SpanSdk class

    Returns:
        ReadableSpan: a new readable span that redacts sensitive data
    """
    # remove sensitive information in span status description
    # this avoid statusMessage on signoz contains sensitive information
    if self._status.status_code == trace.StatusCode.ERROR:
        status_description_redacted = redact_string(str(self._status.description))
        self._status._description = status_description_redacted

    # remove sensitive information in attributes
    # this avoid exception trace and messages contain sensitive information
    for event in self._events:
        attributes = event.attributes
        redacted_event_attributes = redacted_sensitive_data_in_exception(attributes)
        event._name = redact_string(event.name)
        event._attributes = redacted_event_attributes
    return original_function_readable_span(self)


def set_up_logging(session: requests.Session) -> None:
    """Set up logging to export to OTLP."""
    logging_export = os.environ.get("LOGGING_EXPORT_FORMAT", None)
    logging_service_name = os.environ.get("LOGGING_SERVICE_NAME", "schematic-api")
    deployment_environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", "")
    service_instance_id = os.environ.get("SERVICE_INSTANCE_ID", "")
    if logging_export == "otlp":
        resource = Resource.create(
            {
                SERVICE_INSTANCE_ID: service_instance_id,
                SERVICE_NAME: logging_service_name,
                DEPLOYMENT_ENVIRONMENT: deployment_environment,
                SERVICE_VERSION: __version__,
            }
        )

        logger_provider = LoggerProvider(resource=resource)
        set_logger_provider(logger_provider=logger_provider)

        exporter = OTLPLogExporter(session=session)
        logger_provider.add_log_record_processor(CustomFilter(exporter))
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)


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


request_session = create_telemetry_session()
set_up_tracing(session=request_session)
set_up_logging(session=request_session)
