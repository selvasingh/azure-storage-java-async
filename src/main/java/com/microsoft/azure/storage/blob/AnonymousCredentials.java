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

import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;

/**
 * Anonymous credentials are to be used with with HTTP(S) requests
 * that read blobs from public containers or requests that use a
 * Shared Access Signature (SAS).
 */
public final class AnonymousCredentials implements ICredentials {

    /**
     * Anonymous credentials are to be used with with HTTP(S) requests
     * that read blobs from public containers or requests that use a
     * Shared Access Signature (SAS).
     */
    private final class AnonymousCredentialsPolicy implements RequestPolicy {
        final RequestPolicy requestPolicy;

        AnonymousCredentialsPolicy(RequestPolicy requestPolicy) {
            this.requestPolicy = requestPolicy;
        }

        /**
         * For anonymous credentials, this is effectively a no-op
         * @param request
         * @return
         */
        public Single<HttpResponse> sendAsync(HttpRequest request) { return requestPolicy.sendAsync(request); }
    }

    /**
     * Creates a new <code>AnonymousCredentialsPolicy</code>
     * @param nextRequestPolicy
     * @return
     */
    @Override
    public RequestPolicy create(RequestPolicy nextRequestPolicy, RequestPolicyOptions options) {
        return new AnonymousCredentialsPolicy(nextRequestPolicy);
    }
}