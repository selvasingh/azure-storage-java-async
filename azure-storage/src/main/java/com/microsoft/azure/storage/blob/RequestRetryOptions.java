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

import java.util.concurrent.TimeUnit;

/**
 * Options for retrying requests
 */
public final class RequestRetryOptions {

    public static final RequestRetryOptions DEFAULT = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 0,
            0,null, null, null);

    /**
     * A {@link RetryPolicyType} telling the pipeline what kind of retry policy to use.
     */
    private RetryPolicyType retryPolicyType = RetryPolicyType.EXPONENTIAL;

    private int maxTries = 4;

    private int tryTimeout = 30;

    private long retryDelayInMs = TimeUnit.SECONDS.toMillis(4);

    private long maxRetryDelayInMs = TimeUnit.SECONDS.toMillis(120);

    private String secondaryHost;

    /**
     * Configures how the {@link com.microsoft.rest.v2.http.HttpPipeline} should retry requests.
     *
     * @param retryPolicyType
     *      A {@link RetryPolicyType} specifying the type of retry pattern to use.
     * @param maxTries
     *      Specifies the maximum number of attempts an operation will be tried before producing an error
     *      (0=default). A value of {@code null} means that you accept our default policy. A value of 1 means 1 try and no
     *      retries.
     * @param tryTimeout
     *      Indicates the maximum time allowed for any single try of an HTTP request.
     *      A value of {@code null} means that you accept our default timeout. NOTE: When transferring large amounts
     *      of data, the default TryTimeout will probably not be sufficient. You should override this value
     *      based on the bandwidth available to the host machine and proximity to the Storage service. A good
     *      starting point may be something like (60 seconds per MB of anticipated-payload-size).
     * @param retryDelayInMs
     *      Specifies the amount of delay to use before retrying an operation (0=default).
     *      The delay increases (exponentially or linearly) with each retry up to a maximum specified by
     *      MaxRetryDelay. If you specify 0, then you must also specify 0 for MaxRetryDelay.
     * @param maxRetryDelayInMs
     *      MaxRetryDelay specifies the maximum delay allowed before retrying an operation (0=default).
     *      If you specify 0, then you must also specify 0 for RetryDelay.
     * @param secondaryHost
     *      RetryReadsFromSecondaryHost specifies whether the retry policy should retry a read operation against another
     *      host. If RetryReadsFromSecondaryHost is {@code null} (the default) then operations are not retried against another
     *      host. NOTE: Before setting this field, make sure you understand the issues around reading stale &
     *      potentially-inconsistent data at this webpage:
     *      https://docs.microsoft.com/en-us/azure/storage/common/storage-designing-ha-apps-with-ragrs
     */
    public RequestRetryOptions(RetryPolicyType retryPolicyType, int maxTries, int tryTimeout,
                               Long retryDelayInMs, Long maxRetryDelayInMs, String secondaryHost) {
        this.retryPolicyType = retryPolicyType;
        if (maxTries != 0) {
            Utility.assertInBounds("maxRetries", maxTries, 1, Integer.MAX_VALUE);
            this.maxTries = maxTries;
        }

        if (tryTimeout != 0) {
            Utility.assertInBounds("tryTimeoutInMs", tryTimeout, 1, Long.MAX_VALUE);
            this.tryTimeout = tryTimeout;
        }

        if (retryDelayInMs != null && maxRetryDelayInMs != null) {
            Utility.assertInBounds("maxRetryDelayInMs", maxRetryDelayInMs, 1, Long.MAX_VALUE);
            Utility.assertInBounds("retryDelayInMs", retryDelayInMs, 1, maxRetryDelayInMs);
            this.maxRetryDelayInMs = maxRetryDelayInMs;
            this.retryDelayInMs = retryDelayInMs;
        }
        else if (retryDelayInMs != null) {
            Utility.assertInBounds("retryDelayInMs", retryDelayInMs, 1, Long.MAX_VALUE);
            this.retryDelayInMs = retryDelayInMs;
            if (retryDelayInMs > this.maxRetryDelayInMs) {
                this.maxRetryDelayInMs = retryDelayInMs;
            }
        }
        else {
            this.retryDelayInMs = Math.min(this.retryDelayInMs, this.maxRetryDelayInMs);
        }
    }

    /**
     * @return
     *      MaxTries specifies the maximum number of attempts an operation will be tried before producing an error
     *      (0=default). A value of zero means that you accept our default policy. A value of 1 means 1 try and no
     *      retries.
     */
    public int getMaxTries() {
        return this.maxTries;
    }

    /**
     * @return
     *      TryTimeout indicates the maximum time in seconds allowed for any single try of an HTTP request.
     *      A value of zero means that you accept our default timeout. NOTE: When transferring large amounts
     *      of data, the default TryTimeout will probably not be sufficient. You should override this value
     *      based on the bandwidth available to the host machine and proximity to the Storage service. A good
     *      starting point may be something like (60 seconds per MB of anticipated-payload-size).
     */
    public int getTryTimeout() {
        return this.tryTimeout;
    }

    /**
     * @return
     *      RetryReadsFromSecondaryHost specifies whether the retry policy should retry a read operation against another
     *      host. If RetryReadsFromSecondaryHost is {@code null} (the default) then operations are not retried against another
     *      host. NOTE: Before setting this field, make sure you understand the issues around reading stale &
     *      potentially-inconsistent data at this webpage:
     *      https://docs.microsoft.com/en-us/azure/storage/common/storage-designing-ha-apps-with-ragrs
     */
    public String getSecondaryHost() {
        return this.secondaryHost;
    }

    /**
     * Calculates how long to delay before sending the next request.
     *
     * @param tryCount
     *      An {@code int} indicating which try we are on.
     * @return
     *      A {@code long} value of how many milliseconds to delay.
     */
    public long calculatedDelayInMs(int tryCount) {
        long delay = 0;
        switch (this.retryPolicyType) {
            case EXPONENTIAL:
                delay = (pow(2L, tryCount - 1) - 1L) * this.retryDelayInMs;
                break;

            case FIXED:
                delay = this.retryDelayInMs;
                break;
        }

        return delay;
    }

    private long pow(long number, int exponent) {
        long result = 1;
        for (int i = 0; i < exponent; i++) {
            result *= number;
        }

        return result;
    }
}
