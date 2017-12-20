/**
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.azure.storage.blob;

import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.netty.handler.codec.http.HttpResponseStatus;
//import org.apache.log4j.Level;
import io.netty.handler.codec.http.HttpStatusClass;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;

import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Factory for logging requests and responses
 */
public final class LoggingFactory implements RequestPolicyFactory {

    private final LoggingOptions loggingOptions;

    public LoggingFactory(LoggingOptions loggingOptions) {
        this.loggingOptions = loggingOptions;
    }

    private final class LoggingPolicy implements RequestPolicy {

        private int tryCount;

        private long operationStartTime;

        private RequestPolicyOptions options;

        private final RequestPolicy requestPolicy;

        final private LoggingFactory factory;

        private Long requestStartTime;

        LoggingPolicy(RequestPolicy requestPolicy, RequestPolicyOptions options, LoggingFactory factory) {
            this.requestPolicy = requestPolicy;
            this.options = options;
            this.factory = factory;
        }

        /**
         * Signed the request
         * @param request
         *      the request to sign
         * @return
         *      A {@link Single} representing the HTTP response that will arrive asynchronously.
         */
        @Override
        public Single<HttpResponse> sendAsync(final HttpRequest request) {
            this.tryCount++;
            this.requestStartTime = System.currentTimeMillis();
            if (this.tryCount == 1) {
                this.operationStartTime = requestStartTime;
            }

            if (this.options.shouldLog(HttpPipelineLogLevel.INFO)) {
                this.options.log(HttpPipelineLogLevel.INFO,
                        "'%s'==> OUTGOING REQUEST (Try number='%d')%n", request.url(), this.tryCount);
            }

            // TODO: Need to change logic slightly when support for writing to event log/sys log support is added
            return requestPolicy.sendAsync(request)
                    .doOnError(new Consumer<Throwable>() {
                        @Override
                        public void accept(Throwable throwable) {
                            if (options.shouldLog(HttpPipelineLogLevel.ERROR)) {
                                options.log(HttpPipelineLogLevel.ERROR,
                                        "Unexpected failure attempting to make request.%nError message:'%s'%n",
                                        throwable.getMessage());
                            }
                        }
                    })
                    .doOnSuccess(new Consumer<HttpResponse>() {
                        @Override
                        public void accept(HttpResponse response) {
                            long requestEndTime = System.currentTimeMillis();
                            long requestCompletionTime = requestEndTime - requestStartTime;
                            long operationDuration = requestEndTime - operationStartTime;
                            HttpPipelineLogLevel currentLevel = HttpPipelineLogLevel.INFO;
                            // check if error should be logged since there is nothing of higher priority
//                            if (!options.logger().shouldLog(LogLevel.ERROR)) {
//                                return;
//                            }

                            String logMessage = Constants.EMPTY_STRING;
                            //if (options.logger().shouldLog(LogLevel.INFO)) {
                                // assume success and default to informational logging
                                logMessage = "Successfully Received Response" + System.lineSeparator();
                            //}

                            // if the response took too long, we'll upgrade to warning.
                            boolean forceLog = false;
                            if (requestCompletionTime >=
                                    factory.loggingOptions.getMinDurationToLogSlowRequestsInMs()) {
                                // log a warning if the try duration exceeded the specified threshold
                                //if (options.logger().shouldLog(LogLevel.WARNING)) {
                                    currentLevel = HttpPipelineLogLevel.WARNING;
                                    forceLog = true;
                                    logMessage = String.format("SLOW OPERATION. Duration > %d ms.%n", factory.loggingOptions.getMinDurationToLogSlowRequestsInMs());
                                //}
                            }

                            if (response.statusCode() >= HttpURLConnection.HTTP_INTERNAL_ERROR ||
                                    (response.statusCode() >= HttpURLConnection.HTTP_BAD_REQUEST && response.statusCode() != HttpURLConnection.HTTP_NOT_FOUND &&
                                     response.statusCode() != HttpURLConnection.HTTP_CONFLICT && response.statusCode() != HttpURLConnection.HTTP_PRECON_FAILED &&
                                     response.statusCode() != 416 /* 416 is missing from the Enum but it is Range Not Satisfiable */)) {
                                String errorString = String.format("REQUEST ERROR%nHTTP request failed with status code:'%d'%n", response.statusCode());
                                if (currentLevel == HttpPipelineLogLevel.WARNING) {
                                    logMessage += errorString;
                                }
                                else {
                                    logMessage = errorString;
                                }

                                currentLevel = HttpPipelineLogLevel.ERROR;
                                forceLog = true;
                                // TODO: LOG THIS TO WINDOWS EVENT LOG/SYS LOG
                            }

                            //if (shouldlog(currentLevel) {
                                String messageInfo = String.format(
                                        "Request try:'%d', request duration:'%d' ms, operation duration:'%d' ms%n",
                                        tryCount, requestCompletionTime, operationDuration);
                                options.log(HttpPipelineLogLevel.INFO, logMessage + messageInfo);
                            //}
                        }
                    });
        }
    }

    @Override
    public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
        return new LoggingPolicy(next, options, this);
    }
}
