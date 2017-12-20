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
 * Blob HTTP headers for getting and setting blob properties
 */
public final class BlobHttpHeaders {

    private static BlobHttpHeaders defaultBlobHttpHeaders;

    private final String cacheControl;

    private final String contentDisposition;

    private final String contentEncoding;

    private final String contentLanguage;

    private final String contentMD5;

    private String contentType;

    /**
     * A {@link BlobHttpHeaders} object.
     * @param cacheControl
     *      A {@code String} representing the cache-control value stored for the blob.
     * @param contentDisposition
     *      A {@code String} representing the content-disposition value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     * @param contentEncoding
     *      A {@code String} the content-encoding value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     * @param contentLanguage
     *      A {@code String} representing the content-language value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     * @param contentMD5
     *      A {@code String} representing the content MD5 value stored for the blob.
     * @param contentType
     *      A {@code String} representing the content type value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     */
    public BlobHttpHeaders(String cacheControl, String contentDisposition, String contentEncoding,
                           String contentLanguage, String contentMD5, String contentType) {
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
        this.contentEncoding = contentEncoding;
        this.contentLanguage = contentLanguage;
        this.contentMD5 = contentMD5;
        this.contentType = contentType;
    }

    /**
     * @return
     *      A {@code String} representing the cache-control value stored for the blob.
     */
    public String getCacheControl() {
        return cacheControl;
    }

    /**
     * @return
     *      A {@code String} representing the content-disposition value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     */
    public String getContentDisposition() {
        return contentDisposition;
    }

    /**
     * @return
     *      A {@code String} the content-encoding value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     */
    public String getContentEncoding() {
        return contentEncoding;
    }

    /**
     * @return
     *      A {@code String} representing the content-language value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     */
    public String getContentLanguage() {
        return contentLanguage;
    }

    /**
     * @return
     *      A {@code String} representing the content MD5 value stored for the blob.
     */
    public String getContentMD5() {
        return contentMD5;
    }

    /**
     * @return
     *      A {@code String} representing the content type value stored for the blob.
     *      If this field has not been set for the blob, the field returns <code>null</code>.
     */
    public String getContentType() {
        return contentType;
    }

    public static BlobHttpHeaders getDefault() {
        if(defaultBlobHttpHeaders == null) {
            defaultBlobHttpHeaders = new BlobHttpHeaders(null, null, null,
                    null, null, null);
        }
        return defaultBlobHttpHeaders;
    }
}
