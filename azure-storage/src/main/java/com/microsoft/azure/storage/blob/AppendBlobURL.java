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
import com.microsoft.azure.storage.models.AppendBlobsAppendBlockHeaders;
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
public final class AppendBlobURL extends BlobURL {

    /**
     * Creates a new {@link AppendBlobURL} object.
     * @param url
     *      A {@code String} representing a URL to a page blob.
     * @param pipeline
     *      A {@link HttpPipeline} object representing the pipeline for requests.
     */
    public AppendBlobURL(String url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link AppendBlobURL} with the given pipeline.
     * @param pipeline
     *      A {@link HttpPipeline} object to set.
     * @return
     *      A {@link AppendBlobURL} object with the given pipeline.
     */
    public AppendBlobURL withPipeline(HttpPipeline pipeline) {
        return new AppendBlobURL(super.url, pipeline);
    }

    /**
     * Creates a new {@link AppendBlobURL} with the given snapshot.
     * @param snapshot
     *      A <code>java.util.Date</code> to set.
     * @return
     *      A {@link BlobURL} object with the given pipeline.
     */
    public AppendBlobURL withSnapshot(Date snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.ParseURL(super.url);
        blobURLParts.setSnapshot(snapshot);
        return new AppendBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * Create creates a 0-length append blob. Call AppendBlock to append data to an append blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     * @param headers
     *            A {@Link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *            A {@Link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessConditions
     *            A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *            complete.
     * @return the {@link Single&lt;RestResponse&lt;BlobsPutHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobsPutHeaders, Void>> createBlobAsync(
            Metadata metadata, BlobHttpHeaders headers, BlobAccessConditions accessConditions) {
        if(metadata == null) {
            metadata = Metadata.getDefault();
        }
        if(headers == null) {
            headers = BlobHttpHeaders.getDefault();
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.blobs().putWithRestResponseAsync(this.url, BlobType.APPEND_BLOB, null,
                null, headers.getCacheControl(), headers.getContentType(), headers.getContentEncoding(),
                headers.getContentLanguage(), headers.getContentMD5(), headers.getCacheControl(), metadata.toString(),
                accessConditions.getLeaseAccessConditions().toString(),
                headers.getContentDisposition(), accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null, null, null);
    }

    /**
     * AppendBlock commits a new block of data to the end of the existing append blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/append-block.
     * @param data
     *            A <code>byte</code> array which represents the data to write to the blob.
     * @param blobAccessConditions
     *            A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *            complete.
     * @return the {@link Single&lt;RestResponse&lt;AppendBlobsAppendBlockHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<AppendBlobsAppendBlockHeaders, Void>> appendBlockAsync(
            byte[] data, BlobAccessConditions blobAccessConditions) {
        if(blobAccessConditions == null) {
            blobAccessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.appendBlobs().appendBlockWithRestResponseAsync(this.url, data, null,
                blobAccessConditions.getLeaseAccessConditions().toString(),
                blobAccessConditions.getAppendBlobAccessConditions().getIfMaxSizeLessThanOrEqual(),
                blobAccessConditions.getAppendBlobAccessConditions().getIfAppendPositionEquals(),
                blobAccessConditions.getHttpAccessConditions().getIfModifiedSince(),
                blobAccessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                blobAccessConditions.getHttpAccessConditions().getIfMatch().toString(),
                blobAccessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }
}
