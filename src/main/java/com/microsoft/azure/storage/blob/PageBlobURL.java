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

import com.microsoft.azure.storage.implementation.StorageClientImpl;
import com.microsoft.azure.storage.models.BlobType;
import com.microsoft.azure.storage.models.BlobsPutHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Date;

/**
 * Represents a URL to a page blob.
 */
public final class PageBlobURL extends BlobURL {

    /**
     * Creates a new {@link PageBlobURL} object.
     * @param url
     *      A {@code String} representing a URL to a page blob.
     * @param pipeline
     *      A {@link HttpPipeline} object representing the pipeline for requests.
     */
    public PageBlobURL(String url, HttpPipeline pipeline) {
        super( url, pipeline);
    }

    /**
     * Creates a new {@link PageBlobURL} with the given pipeline.
     * @param pipeline
     *      A {@link HttpPipeline} object to set.
     * @return
     *      A {@link PageBlobURL} object with the given pipeline.
     */
    public PageBlobURL withPipeline(HttpPipeline pipeline) {
        return new PageBlobURL(super.url, pipeline);
    }

    /**
     * Creates a new {@link PageBlobURL} with the given snapshot.
     * @param snapshot
     *      A <code>java.util.Date</code> to set.
     * @return
     *      A {@link PageBlobURL} object with the given pipeline.
     */
    public PageBlobURL withSnapshot(Date snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.ParseURL(super.url);
        blobURLParts.setSnapshot(snapshot);
        return new PageBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * Create creates a page blob of the specified length. Call PutPage to upload data data to a page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     * @param size
     *           specifies the maximum size for the page blob, up to 8 TB. The page blob size must be aligned to a
     *           512-byte boundary.
     * @param sequenceNumber
     *            a user-controlled value that you can use to track requests. The value of the sequence number must be
     *            between 0 and 2^63 - 1.The default value is 0.
     * @param blobHttpHeaders
     *            A {@Link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *            A {@Link Metadata} object that specifies key value pairs to set on the blob.
     * @param blobAccessConditions
     *            A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *            complete.
     * @return the {@link Single &lt;RestResponse&lt;BlobsPutHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobsPutHeaders, Void>> putBlobAsync(
            Long size, Long sequenceNumber, Metadata metadata, BlobHttpHeaders blobHttpHeaders,
            BlobAccessConditions blobAccessConditions) {
        if(metadata == null) {
            metadata = Metadata.getDefault();
        }
        if(blobHttpHeaders == null) {
            blobHttpHeaders = BlobHttpHeaders.getDefault();
        }
        if(blobAccessConditions == null) {
            blobAccessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.blobs().putWithRestResponseAsync(this.url, BlobType.PAGE_BLOB, null,
                null, null, blobHttpHeaders.getContentType(), blobHttpHeaders.getContentEncoding(),
                blobHttpHeaders.getContentLanguage(), blobHttpHeaders.getContentMD5(), blobHttpHeaders.getCacheControl(),
                metadata.toString(), blobAccessConditions.getLeaseAccessConditions().toString(),
                blobHttpHeaders.getContentDisposition(),
                blobAccessConditions.getHttpAccessConditions().getIfModifiedSince(),
                blobAccessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                blobAccessConditions.getHttpAccessConditions().getIfMatch().toString(),
                blobAccessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), size, sequenceNumber, null);
    }
}
