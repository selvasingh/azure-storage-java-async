package com.microsoft.azure.storage.blob;

import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipelineLogger;

public final class PipelineOptions {

    // Log configures the pipeline's logging infrastructure indicating what information is logged and where.
    public HttpClient client;

    public HttpPipelineLogger logger;

    // Retry configures the built-in retry policy behavior.
    public RequestRetryOptions requestRetryOptions;

    // configures the built-in request logging policy.
    public LoggingOptions loggingOptions;

    // Telemetry configures the built-in telemetry policy behavior.
    public TelemetryOptions telemetryOptions;
}
