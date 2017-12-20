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

import com.microsoft.rest.v2.http.*;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;

import java.io.IOException;

/**
 * Factory for retrying requests
 */
public final class RequestRetryFactory implements RequestPolicyFactory {

    private final RequestRetryOptions requestRetryOptions;

    public RequestRetryFactory(RequestRetryOptions requestRetryOptions) {
        this.requestRetryOptions = requestRetryOptions;
    }

    private final class RequestRetryPolicy implements RequestPolicy {

        private final RequestPolicy requestPolicy;

        final private RequestRetryOptions requestRetryOptions;

        final private RequestPolicyOptions options;

        private int tryCount;

        private long operationStartTime;

        private HttpRequest httpRequest;

        RequestRetryPolicy(RequestPolicy requestPolicy, RequestPolicyOptions options, RequestRetryOptions requestRetryOptions) {
            this.requestPolicy = requestPolicy;
            this.options = options;
            this.requestRetryOptions = requestRetryOptions;
        }

        @Override
        public Single<HttpResponse> sendAsync(HttpRequest httpRequest) {
            try {
                this.httpRequest = httpRequest.buffer();
            } catch (IOException e) {
                return Single.error(e);
            }

            this.httpRequest = new HttpRequest(httpRequest.callerMethod(), httpRequest.httpMethod(), httpRequest.url(),
                    httpRequest.headers(), httpRequest.body());
            return this.requestPolicy.sendAsync(httpRequest)
                .doOnError(new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) throws Exception {

                    }
                })
                .doOnSuccess(new Consumer<HttpResponse>() {
                    @Override
                    public void accept(HttpResponse httpResponse) throws Exception {
                        long requestEndTime = System.currentTimeMillis();
                        //long requestCompletionTime = requestEndTime - requestStartTime;
                        long operationDuration = requestEndTime - operationStartTime;
                    }
                });
        }
    }

    @Override
    public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
        return new RequestRetryPolicy(next, options, this.requestRetryOptions);
    }
}
