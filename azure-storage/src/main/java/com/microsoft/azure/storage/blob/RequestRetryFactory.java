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
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Factory for retrying requests
 */
public final class RequestRetryFactory implements RequestPolicyFactory {

    private final RequestRetryOptions requestRetryOptions;

    /**
     * Creates a factory capable of generating RequestRetry policies for the {@link HttpPipeline}.
     *
     * @param requestRetryOptions
     *      A {@link RequestRetryOptions} object configuring this factory and all its resultant policies.
     */
    public RequestRetryFactory(RequestRetryOptions requestRetryOptions) {
        this.requestRetryOptions = requestRetryOptions;
    }

    private final class RequestRetryPolicy implements RequestPolicy {

        private final RequestPolicy requestPolicy;

        final private RequestRetryOptions requestRetryOptions;

        private long operationStartTime;

        RequestRetryPolicy(RequestPolicy requestPolicy,
                           RequestRetryOptions requestRetryOptions) {
            this.requestPolicy = requestPolicy;
            this.requestRetryOptions = requestRetryOptions;
        }

        @Override
        public Single<HttpResponse> sendAsync(HttpRequest httpRequest) {
            int primaryTry = 1;

            boolean considerSecondary = (httpRequest.httpMethod().equals("GET") ||
                    httpRequest.httpMethod().equals("HEAD"))
                    && this.requestRetryOptions.secondaryHost != null;

            return this.attemptAsync(httpRequest, primaryTry, considerSecondary, 0)
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
                        // TODO: For logging if slow?
                        long operationDuration = requestEndTime - operationStartTime;
                    }
                });
        }

        private void logf(String s, Object... args) {
            System.out.println(String.format(s, args));
        }

        private Single<HttpResponse> attemptAsync(final HttpRequest httpRequest, final int primaryTry,
                                                  final boolean considerSecondary,
                                                  final int attempt) {
            logf("\n=====> Try=%d\n", attempt);

            final boolean tryingPrimary = !considerSecondary || (attempt%2 == 1);
            long delayMs;
            if(tryingPrimary) {
                delayMs = this.requestRetryOptions.calculatedDelayInMs(primaryTry);
                logf("Primary try=%d, Delay=%v\n", primaryTry, delayMs);
            }
            else {
                delayMs = (long)((ThreadLocalRandom.current().nextFloat()/2+0.8) * 1000); // Add jitter
                logf("Secondary try=%d, Delay=%v\n", attempt-primaryTry, delayMs);
            }

            final HttpRequest requestCopy = httpRequest.buffer();
            if(!tryingPrimary) {
                UrlBuilder builder = UrlBuilder.parse(requestCopy.url());
                builder.withHost(this.requestRetryOptions.secondaryHost);
                try {
                    requestCopy.withUrl(builder.toURL());
                } catch (MalformedURLException e) {
                    return Single.error(e);
                }
            }

            // Deadline stuff

            return Completable.complete().delay(delayMs, TimeUnit.SECONDS)
                    .andThen(this.requestPolicy.sendAsync(requestCopy)
                    .timeout(this.requestRetryOptions.tryTimeout, TimeUnit.SECONDS)
                    .flatMap(new Function<HttpResponse, Single<? extends HttpResponse>>() {
                @Override
                public Single<? extends HttpResponse> apply(HttpResponse httpResponse) throws Exception {
                    boolean newConsiderSecondary = considerSecondary;
                    String action;
                    if(!tryingPrimary && httpResponse.statusCode() == 404) {
                        newConsiderSecondary = false;
                        action = "Retry: Secondary URL returned 404";
                    }
                    else if(httpResponse.statusCode() == 503 || httpResponse.statusCode() == 500) {
                        action = "Retry: Temporary error or timeout";
                    }
                    else {
                        action = "NoRetry: Successful HTTP request";
                    }

                    logf("Action=%s\n", action);

                    if(action.charAt(0)=='R' && attempt <= requestRetryOptions.maxTries) {
                        int newPrimaryTry = tryingPrimary ? primaryTry+1 : primaryTry;
                        return attemptAsync(httpRequest, newPrimaryTry, newConsiderSecondary, attempt+1);
                    }
                    return Single.just(httpResponse);
                }
            }).onErrorResumeNext(new Function<Throwable, SingleSource<? extends HttpResponse>>() {
                @Override
                public SingleSource<? extends HttpResponse> apply(Throwable throwable) throws Exception {
                    if (throwable instanceof IOException && attempt <= requestRetryOptions.maxTries) {
                        int newPrimaryTry = tryingPrimary ? primaryTry+1 : primaryTry;
                        return attemptAsync(httpRequest, newPrimaryTry, considerSecondary, attempt+1);
                    }
                    throw Exceptions.propagate(throwable);
                }
            }));
        }
    }

    @Override
    public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
        return new RequestRetryPolicy(next, this.requestRetryOptions);
    }
}
