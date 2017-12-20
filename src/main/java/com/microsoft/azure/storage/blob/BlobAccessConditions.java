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
 * Optional access conditions which are specific to blobs.
 */
public final class BlobAccessConditions {

    private static BlobAccessConditions defaultBlobAccessConditions;

    // Optional standard HTTP access conditions which are optionally set
    private final HttpAccessConditions httpAccessConditions;

    // Optional access conditions for a lease on a container or blob
    private final LeaseAccessConditions leaseAccessConditions;

    // Optional access conditions which are specific to append blobs
    private final AppendBlobAccessConditions appendBlobAccessConditions;

    // Optional access conditions which are specific to page blobs
    private final PageBlobAccessConditions pageBlobAccessConditions;

    /**
     * Access conditions which are specific to blobs. Passing in null for one of the parameters will set it to a
     * default value.
     * @param httpAccessConditions
     *      Optional standard HTTP access conditions which are optionally set
     * @param leaseAccessConditions
     *      Optional access conditions for a lease on a container or blob
     * @param appendBlobAccessConditions
     *      Optional access conditions which are specific to append blobs
     * @param pageBlobAccessConditions
     *      Optional access conditions which are specific to page blobs
     */
    public BlobAccessConditions(
            HttpAccessConditions httpAccessConditions,
            LeaseAccessConditions leaseAccessConditions,
            AppendBlobAccessConditions appendBlobAccessConditions,
            PageBlobAccessConditions pageBlobAccessConditions) {
        this.httpAccessConditions = httpAccessConditions == null ? HttpAccessConditions.getDefault() : httpAccessConditions;
        this.leaseAccessConditions = leaseAccessConditions == null ? LeaseAccessConditions.getDefault() : leaseAccessConditions;
        this.appendBlobAccessConditions = appendBlobAccessConditions == null ? AppendBlobAccessConditions.getDefault() : appendBlobAccessConditions;
        this.pageBlobAccessConditions = pageBlobAccessConditions == null ? PageBlobAccessConditions.getDefault() : pageBlobAccessConditions;
    }

    HttpAccessConditions getHttpAccessConditions() {
        return httpAccessConditions;
    }

    LeaseAccessConditions getLeaseAccessConditions() {
        return leaseAccessConditions;
    }

    AppendBlobAccessConditions getAppendBlobAccessConditions() {
        return appendBlobAccessConditions;
    }

    PageBlobAccessConditions getPageBlobAccessConditions() {
        return pageBlobAccessConditions;
    }

    public static BlobAccessConditions getDefault() {
        if (defaultBlobAccessConditions == null) {
            defaultBlobAccessConditions = new BlobAccessConditions(HttpAccessConditions.getDefault(),
                    LeaseAccessConditions.getDefault(),
                    AppendBlobAccessConditions.getDefault(),
                    PageBlobAccessConditions.getDefault());
        }

        return defaultBlobAccessConditions;
    }
}
