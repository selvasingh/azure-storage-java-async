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

import com.microsoft.azure.storage.models.AppendBlobAppendBlockHeaders;
import com.microsoft.azure.storage.models.BlobType;
import com.microsoft.azure.storage.models.BlobPutHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;


/**
 * Represents a URL to a append blob.
 */
public final class AppendBlobURL extends BlobURL {

    /**
     * Creates a new {@link AppendBlobURL} object.
     *
     * @param url
     *      A {@code java.net.URL} to a page blob.
     * @param pipeline
     *      An {@link HttpPipeline} object representing the pipeline for requests.
     */
    public AppendBlobURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link AppendBlobURL} with the given pipeline.
     *
     * @param pipeline
     *      An {@link HttpPipeline} object to set.
     * @return
     *      An {@link AppendBlobURL} object with the given pipeline.
     */
    public AppendBlobURL withPipeline(HttpPipeline pipeline) {
        try {
            return new AppendBlobURL(new URL(this.storageClient.url()), pipeline);
        }
        catch (MalformedURLException e) {
            //TODO: remove
        }
        return null;
    }

    /**
     * Creates a new {@link AppendBlobURL} with the given snapshot.
     *
     * @param snapshot
     *      A {@code String} to set.
     * @return
     *      A {@link BlobURL} object with the given pipeline.
     */
    public AppendBlobURL withSnapshot(String snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.parse(new URL(this.storageClient.url()));
        blobURLParts.setSnapshot(snapshot);
        return new AppendBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * Create creates a 0-length append blob. Call AppendBlock to append data to an append blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     *
     * @param headers
     *      A {@link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobPutHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobPutHeaders, Void>> create(
            Metadata metadata, BlobHttpHeaders headers, BlobAccessConditions accessConditions) {
        if(metadata == null) {
            metadata = Metadata.NONE;
        }
        if(headers == null) {
            headers = BlobHttpHeaders.NONE;
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }
        return this.storageClient.blobs().putWithRestResponseAsync(0, BlobType.APPEND_BLOB,
                null,null, headers.getContentType(), headers.getContentEncoding(),
                headers.getContentLanguage(), headers.getContentMD5(), headers.getCacheControl(), metadata.toString(),
                accessConditions.getLeaseAccessConditions().getLeaseId(),
                headers.getContentDisposition(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null, null, null);
    }

    /**
     * AppendBlock commits a new block of data to the end of the existing append blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/append-block.
     *
     * @param data
     *      A {@code Flowable&lt;byte[]&gt;} which represents the data to write to the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;AppendBlobAppendBlockHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<AppendBlobAppendBlockHeaders, Void>> appendBlock(
            Flowable<ByteBuffer> data, long length, BlobAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }

        return this.storageClient.appendBlobs().appendBlockWithRestResponseAsync(data, length, null,
                accessConditions.getLeaseAccessConditions().getLeaseId(),
                accessConditions.getAppendBlobAccessConditions().getIfMaxSizeLessThanOrEqual(),
                accessConditions.getAppendBlobAccessConditions().getIfAppendPositionEquals(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }
}
