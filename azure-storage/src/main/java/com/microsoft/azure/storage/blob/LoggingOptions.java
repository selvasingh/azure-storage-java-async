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

import java.util.logging.Level;

/**
 * Logging options
 */
public final class LoggingOptions {

    private final Long minDurationToLogSlowRequestsInMs;

    private final Level loggingLevel;

    /**
     * Creates a new {@link LoggingOptions} object
     */
    public LoggingOptions() {
        this(Level.SEVERE);
    }

    /**
     * Creates a new {@link LoggingOptions} object
     * @param loggingLevel
     *      The minimum {@code java.util.logging.Level} to log requests
     */
    public LoggingOptions(Level loggingLevel) {
        this(loggingLevel, 3000L);
    }

    /**
     * Creates a new {@link LoggingOptions} object
     * @param loggingLevel
     *      The minimum {@code java.util.logging.Level} to log requests
     * @param minDurationToLogSlowRequestsInMs
     *      A {@code Long} representing the minimum duration for a tried operation to log a warning
     */
    public LoggingOptions(Level loggingLevel, Long minDurationToLogSlowRequestsInMs) {
        this.loggingLevel = loggingLevel;
        this.minDurationToLogSlowRequestsInMs = minDurationToLogSlowRequestsInMs;
    }

    /**
     * @return
     *      The minimum {@code java.util.logging.Level} to log requests
     */
    public Level getLoggingLevel() {
        return loggingLevel;
    }

    /**
     * @return
     *      A {@code Long} representing the minimum duration for a tried operation to log a warning
     */
    public Long getMinDurationToLogSlowRequestsInMs() {
        return minDurationToLogSlowRequestsInMs;
    }


}
