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

import com.microsoft.azure.storage.models.*;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.RestResponse;
import io.reactivex.Single;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.List;

/**
 * Represents a URL to a block blob.
 */
public final class BlockBlobURL extends BlobURL {

    /**
     * Creates a new {@link BlockBlobURL} object.
     *
     * @param url
     *      A {@code String} representing a URL to a block blob.
     * @param pipeline
     *      An {@link HttpPipeline} object representing the pipeline for requests.
     */
    public BlockBlobURL(String url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link BlockBlobURL} with the given pipeline.
     *
     * @param pipeline
     *      An {@link HttpPipeline} object to set.
     * @return
     *      A {@link BlockBlobURL} object with the given pipeline.
     */
    public BlockBlobURL withPipeline(HttpPipeline pipeline) {
        return new BlockBlobURL(this.url, pipeline);
    }

    /**
     * Creates a new {@link BlockBlobURL} with the given snapshot.
     *
     * @param snapshot
     *      A {@code java.util.Date} to set.
     * @return
     *      A {@link BlockBlobURL} object with the given pipeline.
     */
    public BlockBlobURL withSnapshot(String snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.ParseURL(super.url);
        blobURLParts.setSnapshot(snapshot);
        return new BlockBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * PutBlob creates a new block blob, or updates the content of an existing block blob.
     * Updating an existing block blob overwrites any existing metadata on the blob. Partial updates are not
     * supported with PutBlob; the content of the existing blob is overwritten with the new content. To
     * perform a partial update of a block blob's, use PutBlock and PutBlockList.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     *
     * @param data
     *      An {@link AsyncInputStream} which contains the data to write to the blob.
     * @param headers
     *      A {@link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobsPutHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobsPutHeaders, Void>> putBlobAsync(
            AsyncInputStream data, BlobHttpHeaders headers, Metadata metadata, BlobAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        if(headers == null) {
            headers = BlobHttpHeaders.getDefault();
        }
        // TODO: Metadata protocol layer broken.
        if(metadata == null) {
            metadata = Metadata.getDefault();
        }
        return this.storageClient.blobs().putWithRestResponseAsync(this.url, BlobType.BLOCK_BLOB, data,
                null, null, headers.getContentType(), headers.getContentEncoding(),
                headers.getContentLanguage(), headers.getContentMD5(), headers.getCacheControl(), metadata.toString(),
                accessConditions.getLeaseAccessConditions().toString(),
                headers.getContentDisposition(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null, null, null);
    }

    /**
     * PutBlock uploads the specified block to the block blob's "staging area" to be later commited by a call to
     * PutBlockList. For more information, see https://docs.microsoft.com/rest/api/storageservices/put-block.
     *
     * @param base64BlockID
     *      A Base64 encoded {@code String} that specifies the ID for this block.
     * @param data
     *      An {@link AsyncInputStream} which contains the data to write to the block.
     * @param leaseAccessConditions
     *      A {@link LeaseAccessConditions} object that specifies the lease on the blob if there is one.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlockBlobsPutBlockHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlockBlobsPutBlockHeaders, Void>> putBlockAsync(
            String base64BlockID, AsyncInputStream data, LeaseAccessConditions leaseAccessConditions) {
        if(leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.getDefault();
        }
        return this.storageClient.blockBlobs().putBlockWithRestResponseAsync(this.url, base64BlockID, data,
                null, leaseAccessConditions.toString(), null);
    }

    /**
     * GetBlockList returns the list of blocks that have been uploaded as part of a block blob using the specified block
     * list filter. For more information, see https://docs.microsoft.com/rest/api/storageservices/get-block-list.
     * @param listType
     *      A {@link BlockListType} value specifies which type of blocks to return.
     * @param leaseAccessConditions
     *      A {@link LeaseAccessConditions} object that specifies the lease on the blob if there is one.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlockBlobsGetBlockListHeaders, BlockList&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlockBlobsGetBlockListHeaders, BlockList>> getBlockListAsync(
            BlockListType listType, LeaseAccessConditions leaseAccessConditions) {
        if(leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.getDefault();
        }
        return this.storageClient.blockBlobs().getBlockListWithRestResponseAsync(this.url, listType,
                null, null, leaseAccessConditions.toString(), null);
    }

    /**
     * PutBlockList writes a blob by specifying the list of block IDs that make up the blob.
     * In order to be written as part of a blob, a block must have been successfully written
     * to the server in a prior PutBlock operation. You can call PutBlockList to update a blob
     * by uploading only those blocks that have changed, then committing the new and existing
     * blocks together. Any blocks not specified in the block list and permanently deleted.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-block-list.
     *
     * @param base64BlockIDs
     *      A {@code java.util.List} of base64 {@code String} that specifies the block IDs to be committed.
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param httpHeaders
     *      A {@link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlockBlobsPutBlockListHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlockBlobsPutBlockListHeaders, Void>> putBlockListAsync(
            List<String> base64BlockIDs, Metadata metadata, BlobHttpHeaders httpHeaders,
            BlobAccessConditions accessConditions) {
        if(metadata == null) {
            metadata = Metadata.getDefault();
        }
        if(httpHeaders == null) {
            httpHeaders = BlobHttpHeaders.getDefault();
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.blockBlobs().putBlockListWithRestResponseAsync(this.url,
                new BlockLookupList().withLatest(base64BlockIDs), null,
                httpHeaders.getCacheControl(), httpHeaders.getContentType(),httpHeaders.getContentEncoding(),
                httpHeaders.getContentLanguage(), httpHeaders.getContentMD5(), metadata.toString(),
                accessConditions.getLeaseAccessConditions().toString(), httpHeaders.getContentDisposition(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }
}
