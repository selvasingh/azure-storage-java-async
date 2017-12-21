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

import com.microsoft.azure.storage.implementation.StorageClientImpl;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineBuilder;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.Locale;

import static com.microsoft.azure.storage.blob.Utility.getGMTTime;

public abstract class StorageURL {

    protected final String url;

    protected final StorageClientImpl storageClient;

    protected StorageURL(String url, HttpPipeline pipeline) {
        if (url == null) {
            throw new IllegalArgumentException("url cannot be null.");
        }
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline cannot be null.");
        }

        this.url = url;
        this.storageClient = new StorageClientImpl(pipeline).withVersion("2016-05-31");
    }

    // TODO: ADD RETRY Factory
    public static HttpPipeline CreatePipeline(ICredentials credentials, PipelineOptions pipelineOptions) {
        LoggingFactory loggingFactory = new LoggingFactory(pipelineOptions.loggingOptions);
        RequestIDFactory requestIDFactory = new RequestIDFactory();
        //RequestRetryFactory requestRetryFactory = new RequestRetryFactory();
        TelemetryFactory telemetryFactory = new TelemetryFactory(pipelineOptions.telemetryOptions);
        AddDatePolicy addDate = new AddDatePolicy();
        return HttpPipeline.build(
                pipelineOptions.client, requestIDFactory, telemetryFactory, addDate, credentials, loggingFactory);
    }

    @Override
    public String toString() {
        return this.url;
    }

    /**
     * appends a string to the end of a URL's path (prefixing the string with a '/' if required)
     * @param url
     * @param name
     * @return
     */
    protected String appendToURLPath(String url, String name) {
        if (url.length() == 0 || url.charAt(url.length() - 1) != '/') {
            url += '/';
        }
        return url + name;
    }

    static class AddDatePolicy implements RequestPolicyFactory {

        @Override
        public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
            return new AddDate(next);
        }

        public final class AddDate implements RequestPolicy {
            private final DateTimeFormatter format = DateTimeFormat
                    .forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZoneUTC()
                    .withLocale(Locale.US);

            private final RequestPolicy next;
            public AddDate(RequestPolicy next) {
                this.next = next;
            }

            @Override
            public Single<HttpResponse> sendAsync(HttpRequest request) {
                request.headers().set(Constants.HeaderConstants.DATE, getGMTTime(new Date()));
                return this.next.sendAsync(request);
            }
        }
    }
}
