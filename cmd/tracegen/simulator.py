#!/usr/bin/env python3
import random
import time

from opentelemetry import trace, propagate
from opentelemetry.trace import Status, StatusCode, SpanKind
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


# --------------------------------------------------
# Shared Exporter (both services send to same OTLP)
# --------------------------------------------------

exporter = OTLPSpanExporter(
    endpoint="localhost:4317",
    insecure=True
)

# --------------------------------------------------
# Client Service Setup
# --------------------------------------------------
client_provider = TracerProvider(
    resource=Resource.create(
        {
            "service.name": "payment-client",
            "demo.foo": "bar",
            "demo.bar": "baz",
        }
    )
)
client_provider.add_span_processor(BatchSpanProcessor(exporter))
client_tracer = client_provider.get_tracer("payment-client")

# --------------------------------------------------
# Server Service Setup
# --------------------------------------------------

server_provider = TracerProvider(
    resource=Resource.create(
        {
            "service.name": "payment-server",
            "demo.host": "my-host",
            "demo.port": 1234,
        }
    )
)
server_provider.add_span_processor(BatchSpanProcessor(exporter))
server_tracer = server_provider.get_tracer("payment-server")


# --------------------------------------------------
# Server Logic (simulated remote service)
# --------------------------------------------------

def server_handle_request(headers, request_id):

    # Extract incoming context (simulated HTTP headers)
    ctx = propagate.extract(headers)

    with server_tracer.start_as_current_span(
        "POST /process-payment",
        context=ctx,
        kind=SpanKind.SERVER
    ) as span:

        span.set_attribute("request.id", request_id)
        span.add_event("Request received", {"log.level": "INFO"})

        # ---- Internal span: validation ----
        with server_tracer.start_as_current_span(
            "validate-payment",
            kind=SpanKind.INTERNAL
        ) as internal_span:

            internal_span.add_event("Validating input", {"log.level": "DEBUG"})
            time.sleep(random.uniform(0.05, 0.2))

            if random.random() < 0.1:
                error = ValueError("Invalid payment details")
                internal_span.record_exception(error)
                internal_span.set_status(Status(StatusCode.ERROR))
                internal_span.add_event(
                    "Validation failed",
                    {"log.level": "ERROR"}
                )
                raise error

        # ---- Internal span: database call ----
        with server_tracer.start_as_current_span(
            "charge-database",
            kind=SpanKind.INTERNAL
        ) as db_span:

            db_span.add_event("Charging database", {"log.level": "DEBUG"})
            latency = random.uniform(0.1, 0.5)
            time.sleep(latency)
            db_span.set_attribute("db.latency_ms", int(latency * 1000))

            if random.random() < 0.25:
                error = RuntimeError("Database timeout")
                db_span.record_exception(error)
                db_span.set_status(Status(StatusCode.ERROR))
                db_span.add_event(
                    "Database error",
                    {"log.level": "ERROR"}
                )
                raise error

        span.add_event("Payment processed successfully", {"log.level": "INFO"})
        span.set_attribute("http.status_code", 200)

        return {"status": "success"}


# --------------------------------------------------
# Client Logic
# --------------------------------------------------

def client_make_request(request_id):

    with client_tracer.start_as_current_span(
        "POST payment-service",
        kind=SpanKind.CLIENT
    ) as span:

        span.set_attribute("request.id", request_id)
        span.add_event("Sending payment request", {"log.level": "INFO"})

        # Inject context into simulated HTTP headers
        headers = {}
        propagate.inject(headers)

        try:
            response = server_handle_request(headers, request_id)

            span.add_event(
                "Received response",
                {"log.level": "DEBUG", "response.status": response["status"]}
            )

        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR))
            span.add_event(
                "Payment request failed",
                {"log.level": "ERROR", "exception.type": type(e).__name__}
            )


# --------------------------------------------------
# Run Simulation
# --------------------------------------------------

if __name__ == "__main__":

    print("Starting distributed trace simulation...\n")

    for i in range(1, 11):
        client_make_request(i)
        time.sleep(0.5)

    time.sleep(3)
    print("\nSimulation complete. Traces sent to localhost:4317.")
