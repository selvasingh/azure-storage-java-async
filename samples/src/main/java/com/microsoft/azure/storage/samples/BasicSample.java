/**
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlobAccessConditions;
import com.microsoft.azure.storage.blob.BlobHttpHeaders;
import com.microsoft.azure.storage.blob.BlobURL;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.Constants;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.Metadata;
import com.microsoft.azure.storage.models.BlobsPutHeaders;
import com.microsoft.azure.storage.models.ContainerCreateHeaders;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Locale;

public class BasicSample {

    // RequestPolicyFactory creates RequestPolicies. Each RequestPolicy is notified when
    // an HttpRequest is sent out and can modify the outgoing request or incoming response as it sees fit.
    // A RequestPolicy is similar to an Interceptor or Filter in other HTTP clients.
    static class AddDatePolicyFactory implements RequestPolicyFactory {
        private final DateTimeFormatter format = DateTimeFormat
                .forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                .withZoneUTC()
                .withLocale(Locale.US);

        @Override
        public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
            return new AddDatePolicy(next);
        }

        // This RequestPolicy is instantiated for each request which allows it to have internal state
        // e.g. number of retries attempted for a retry policy
        public final class AddDatePolicy implements RequestPolicy {
            private final DateTimeFormatter format = DateTimeFormat
                    .forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZoneUTC()
                    .withLocale(Locale.US);

            // This RequestPolicy decides when and how many times
            private final RequestPolicy next;
            public AddDatePolicy(RequestPolicy next) {
                this.next = next;
            }

            @Override
            public Single<HttpResponse> sendAsync(HttpRequest request) {
                request.headers().set(Constants.HeaderConstants.DATE, format.print(DateTime.now()));
                return this.next.sendAsync(request);
            }
        }
    }

    public static void main(String[] args) {
        // HttpPipeline is where RequestPolicyFactories are added
        HttpPipeline pipeline = HttpPipeline.build(new AddDatePolicyFactory());
        final ContainerURL containerURL = new ContainerURL("http://javabenchmark.blob.core.windows.net/testContainer", pipeline);
        containerURL.createAsync(null, null, null).flatMap(new Function<RestResponse<ContainerCreateHeaders, Void>, Single<RestResponse<BlobsPutHeaders, Void>>>() {
            @Override
            public Single<RestResponse<BlobsPutHeaders, Void>> apply(RestResponse<ContainerCreateHeaders, Void> response) throws Exception {
                if (response.statusCode() == 201) {
                    throw new Exception("Expected status code 201, but got " + response.statusCode());
                }
                BlockBlobURL blobURL = containerURL.createBlockBlobURL("testBlob");

                byte[] data = { 0, 1, 2, 3, 4 };
                InputStream ioStream = new ByteArrayInputStream(data);
                AsyncInputStream asyncStream = AsyncInputStream.create(ioStream, data.length);
                // TODO: AsyncInputStream
                return blobURL.putBlobAsync(data, null, null, null).flatMap(new Function<RestResponse<BlobsPutHeaders, Void>, SingleSource<? extends RestResponse<BlobsPutHeaders, Void>>>() {
                    @Override
                    public SingleSource<? extends RestResponse<BlobsPutHeaders, Void>> apply(RestResponse<BlobsPutHeaders, Void> blobResponse) throws Exception {

                        return null;
                    }
                });
            }
        });
    }
}