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

/**
 * Configures the telemetry policy's behavior.
 */
public final class TelemetryOptions {

    /**
     * userAgentPrefix is a string prepended to each request's User-Agent and sent to the service. The service records.
     * the user-agent in logs for diagnostics and tracking of client requests.
     */
    private final String userAgentPrefix;

    public TelemetryOptions() { this(Constants.EMPTY_STRING); }

    public TelemetryOptions(String userAgentPrefix) {
        this.userAgentPrefix = userAgentPrefix;
    }

    @Override
    public boolean equals(Object obj) {
        if (this.userAgentPrefix == null) {
            return obj == null;
        }

        return this.userAgentPrefix.equals(obj);
    }

    @Override
    public String toString() {
        return this.userAgentPrefix;
    }

    public String UserAgentPrefix() {
        return this.userAgentPrefix;
    }
}
