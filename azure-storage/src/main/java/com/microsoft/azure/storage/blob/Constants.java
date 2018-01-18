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
 * RESERVED FOR INTERNAL USE. Contains storage constants.
 */
public final class Constants {

    /**
     * Defines constants for use with HTTP headers.
     */
    public static class HeaderConstants {
        /**
         * The Authorization header.
         */
        public static final String AUTHORIZATION = "Authorization";

        /**
         * The format string for specifying ranges with only begin offset.
         */
        public static final String BEGIN_RANGE_HEADER_FORMAT = "bytes=%d-";

        /**
         * The header that indicates the client request ID.
         */
        public static final String CLIENT_REQUEST_ID_HEADER = PREFIX_FOR_STORAGE_HEADER + "client-request-id";

        /**
         * The ContentEncoding header.
         */
        public static final String CONTENT_ENCODING = "Content-Encoding";

        /**
         * The ContentLangauge header.
         */
        public static final String CONTENT_LANGUAGE = "Content-Language";

        /**
         * The ContentLength header.
         */
        public static final String CONTENT_LENGTH = "Content-Length";

        /**
         * The ContentMD5 header.
         */
        public static final String CONTENT_MD5 = "Content-MD5";

        /**
         * The ContentType header.
         */
        public static final String CONTENT_TYPE = "Content-Type";

        /**
         * The header that specifies the date.
         */
        public static final String DATE = PREFIX_FOR_STORAGE_HEADER + "date";

        /**
         * The IfMatch header.
         */
        public static final String IF_MATCH = "If-Match";

        /**
         * The IfModifiedSince header.
         */
        public static final String IF_MODIFIED_SINCE = "If-Modified-Since";

        /**
         * The IfNoneMatch header.
         */
        public static final String IF_NONE_MATCH = "If-None-Match";

        /**
         * The IfUnmodifiedSince header.
         */
        public static final String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";

        /**
         * The Range header.
         */
        public static final String RANGE = "Range";

        /**
         * The format string for specifying ranges.
         */
        public static final String RANGE_HEADER_FORMAT = "bytes=%d-%d";

        /**
         * The current storage version header value.
         */
        public static final String TARGET_STORAGE_VERSION = "2017-04-17";

        /**
         * The UserAgent header.
         */
        public static final String USER_AGENT = "User-Agent";

        /**
         * Specifies the value to use for UserAgent header.
         */
        public static final String USER_AGENT_PREFIX = "Azure-Storage-Async";

        /**
         * Specifies the value to use for UserAgent header.
         */
        public static final String USER_AGENT_VERSION = "1.0.0";
    }

    /**
     * The master Microsoft Azure Storage header prefix.
     */
    public static final String PREFIX_FOR_STORAGE_HEADER = "x-ms-";

    /**
     * Constant representing a kilobyte (Non-SI version).
     */
    public static final int KB = 1024;

    /**
     * Constant representing a megabyte (Non-SI version).
     */
    public static final int MB = 1024 * KB;

    /**
     * An empty {@code String} to use for comparison.
     */
    public static final String EMPTY_STRING = "";

    /**
     * Specifies HTTP.
     */
    public static final String HTTP = "http";

    /**
     * Specifies HTTPS.
     */
    public static final String HTTPS = "https";

    /**
     * Specifies both HTTPS and HTTP.
     */
    public static final String HTTPS_HTTP = "https,http";

    /**
     * XML attribute for IDs.
     */
    public static final String ID = "Id";

    /**
     * The default type for content-type and accept
     */
    public static final String UTF8_CHARSET = "UTF-8";

    /**
     * Private Default Ctor
     */
    private Constants() {
        // No op
    }
}