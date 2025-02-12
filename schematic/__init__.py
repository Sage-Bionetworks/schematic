import logging
import re
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
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, SpanExporter
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider, SpanProcessor, Event, ReadableSpan
from opentelemetry.trace import Span, SpanContext, get_current_span, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from synapseclient import Synapse, USER_AGENT
from werkzeug import Request

from schematic.configuration.configuration import CONFIG
from schematic.loader import LOADER
from schematic.version import __version__
from dotenv import load_dotenv

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


class FilterSensitiveDataProcessor(SpanProcessor):
    """A custom span processor that filters out sensitive data from the spans.
    It filters out the data from the attributes and events of the spans.

    Args:
        SpanProcessor (opentelemetry.sdk.trace.SpanProcessor): The base class that provides hooks for processing spans during their lifecycle
    """

    def __init__(self, export: SpanExporter) -> None:
        self.sensitive_patterns = {
            "google_sheets": r"https://sheets\.googleapis\.com/v4/spreadsheets/[\w-]+"
        }

        self._compiled_patterns = {
            name: re.compile(pattern)
            for name, pattern in self.sensitive_patterns.items()
        }
        self.export = export

    def _redact_string(self, value: str) -> str:
        """remove sensitive data from a string

        Args:
            value (str): a string that may contain sensitive data

        Returns:
            str: remove sensitive data from string
        """
        redacted = value
        for pattern_name, pattern in self._compiled_patterns.items():
            redacted = pattern.sub(f"[REDACTED_{pattern_name.upper()}]", redacted)
        return redacted

    def _redacted_sensitive_data_in_exception(
        self, exception_attributes: Dict[str, str]
    ) -> Dict[str, str]:
        """remove sensitive data in exception

        Args:
            exception_attributes (dict):a dictionary of exception attributes

        Returns:
            dict: a dictionary of exception attributes with sensitive data redacted
        """

        redacted_exception_attributes = {}
        for key, value in exception_attributes.items():
            # remove sensitive information from exception message and stacktrace
            if key == "exception.message" or key == "exception.stacktrace":
                redacted_exception_attributes[key] = self._redact_string(value)
            else:
                redacted_exception_attributes[key] = value
        return redacted_exception_attributes

    def _create_redacted_span(self, span: ReadableSpan) -> ReadableSpan:
        redacted_events = []
        for event in span.events:
            attributes = event.attributes
            # remove sensitive data from exception
            redacted_event_attributes = self._redacted_sensitive_data_in_exception(
                attributes
            )
            redacted_event = Event(
                name=self._redact_string(event.name),
                attributes=redacted_event_attributes,
                timestamp=event.timestamp,
            )
            redacted_events.append(redacted_event)
            # Create new span with redacted data
        redacted_span = ReadableSpan(
            name=span.name,
            context=span.get_span_context(),
            parent=span.parent,
            resource=span.resource,
            attributes=span.attributes,
            events=redacted_events,
            links=span.links,
            kind=span.kind,
            status=span.status,
            start_time=span.start_time,
            end_time=span.end_time,
            instrumentation_info=span.instrumentation_info,
        )
        return redacted_span

    def on_end(self, span: ReadableSpan) -> None:
        """Remove sensitive information in spans by creating new ones with redacted data."""
        # maybe the goal should be to filter all spans?
        if span.status.status_code == trace.StatusCode.ERROR:
            if span.events:
                redacted_span = self._create_redacted_span(span)
                self.export.export([redacted_span])

    def shutdown(self):
        """Shuts down the processor and exporter."""
        self._exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000):
        """Forces flush of pending spans."""
        self._exporter.force_flush(timeout_millis)


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

    def redact_google_sheet(self, message: str) -> str:
        """Redacts sensitive patterns from google."""
        pattern = re.compile(r"https://sheets\.googleapis\.com/v4/spreadsheets/[\w-]+")
        sanitized_message = re.sub(pattern, "REDACTED", message)
        return sanitized_message

    def emit(self, log_record: LogRecord) -> None:
        """Modify log traces before they are exported."""
        # Redact sensitive data in the log message (body)
        if log_record.log_record.body and "googleapis" in log_record.log_record.body:
            log_record.log_record.body = self.redact_google_sheet(
                log_record.log_record.body
            )

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
        filter_sensitive_data_processor = FilterSensitiveDataProcessor(exporter)
        trace.get_tracer_provider().add_span_processor(filter_sensitive_data_processor)
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


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
