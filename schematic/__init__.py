import logging
import os
from typing import Dict, List

import requests
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, Span
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from requests_oauth2client import OAuth2Client, OAuth2ClientCredentialsAuth
from synapseclient import Synapse
from werkzeug import Request

from schematic.configuration.configuration import CONFIG
from schematic.loader import LOADER
from schematic_api.api.security_controller import info_from_bearer_auth

Synapse.allow_client_caching(False)
logger = logging.getLogger(__name__)


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

    client_id = os.environ.get("TELEMETRY_EXPORTER_CLIENT_ID", None)
    client_secret = os.environ.get("TELEMETRY_EXPORTER_CLIENT_SECRET", None)
    client_token_endpoint = os.environ.get(
        "TELEMETRY_EXPORTER_CLIENT_TOKEN_ENDPOINT", None
    )
    client_audience = os.environ.get("TELEMETRY_EXPORTER_CLIENT_AUDIENCE", None)
    if (
        not client_id
        or not client_secret
        or not client_token_endpoint
        or not client_audience
    ):
        logger.warning(
            "No client_id, client_secret, client_audience, or token_endpoint provided for telemetry exporter. Telemetry data will be sent without authentication."
        )
        return session

    oauth2client = OAuth2Client(
        token_endpoint=client_token_endpoint,
        client_id=client_id,
        client_secret=client_secret,
    )

    auth = OAuth2ClientCredentialsAuth(client=oauth2client, audience=client_audience)
    session.auth = auth

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
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource(
                    attributes={
                        SERVICE_NAME: tracing_service_name,
                        # TODO: Revisit this portion later on. As of 11/12/2024 when
                        # deploying this to ECS or running within a docker container,
                        # the package version errors out with the following error:
                        # importlib.metadata.PackageNotFoundError: No package metadata was found for schematicpy
                        # SERVICE_VERSION: package_version,
                        DEPLOYMENT_ENVIRONMENT: deployment_environment,
                    }
                )
            )
        )
        FlaskInstrumentor().instrument(
            request_hook=request_hook, response_hook=response_hook
        )

    if tracing_export == "otlp":
        exporter = OTLPSpanExporter(session=session)
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))
    else:
        trace.set_tracer_provider(TracerProvider(sampler=ALWAYS_OFF))


def set_up_logging(session: requests.Session) -> None:
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

        logger_provider = LoggerProvider(resource=resource)
        set_logger_provider(logger_provider=logger_provider)

        exporter = OTLPLogExporter(session=session)
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


request_session = create_telemetry_session()
set_up_tracing(session=request_session)
set_up_logging(session=request_session)
