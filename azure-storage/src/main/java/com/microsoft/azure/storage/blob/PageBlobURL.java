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
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.UrlBuilder;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

/**
 * Represents a URL to a page blob.
 */
public final class PageBlobURL extends BlobURL {

    /**
     * Creates a new {@link PageBlobURL} object.
     *
     * @param url
     *      A {@code java.net.URL} to a page blob.
     * @param pipeline
     *      A {@link HttpPipeline} object representing the pipeline for requests.
     */
    public PageBlobURL(URL url, HttpPipeline pipeline) {
        super( url, pipeline);
    }

    /**
     * Creates a new {@link PageBlobURL} with the given pipeline.
     *
     * @param pipeline
     *      A {@link HttpPipeline} object to set.
     * @return
     *      A {@link PageBlobURL} object with the given pipeline.
     */
    public PageBlobURL withPipeline(HttpPipeline pipeline) {
        try {
            return new PageBlobURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * Creates a new {@link PageBlobURL} with the given snapshot.
     *
     * @param snapshot
     *      A {@code java.util.Date} to set.
     * @return
     *      A {@link PageBlobURL} object with the given pipeline.
     */
    public PageBlobURL withSnapshot(String snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.parse(new URL(this.storageClient.url()));
        blobURLParts.setSnapshot(snapshot);
        return new PageBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * Create creates a page blob of the specified length. Call PutPage to upload data data to a page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     *
     * @param size
     *      Specifies the maximum size for the page blob, up to 8 TB. The page blob size must be aligned to a
     *      512-byte boundary.
     * @param sequenceNumber
     *      A user-controlled value that you can use to track requests. The value of the sequence number must be
     *      between 0 and 2^63 - 1.The default value is 0.
     * @param headers
     *      A {@link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *       The {@link Single &lt;RestResponse&lt;BlobPutHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobPutHeaders, Void>> create(
            long size, Long sequenceNumber, Metadata metadata, BlobHttpHeaders headers,
            BlobAccessConditions accessConditions) {
        if (size%512 != 0) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException("size must be a multiple of 512.");
        }
        if (sequenceNumber != null && sequenceNumber < 0) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException("SequenceNumber must be greater than or equal to 0.");
        }
        if(metadata == null) {
            metadata = Metadata.NONE;
        }
        if(headers == null) {
            headers = BlobHttpHeaders.NONE;
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }

        // TODO: What if you pass 0 for pageblob size? Validate?
        return this.storageClient.blobs().putWithRestResponseAsync(0, BlobType.PAGE_BLOB, null,
                null, headers.getContentType(), headers.getContentEncoding(),
                headers.getContentLanguage(), headers.getContentMD5(), headers.getCacheControl(),
                metadata.toString(), accessConditions.getLeaseAccessConditions().getLeaseId(),
                headers.getContentDisposition(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                size, sequenceNumber, null);
    }

    /**
     * PutPages writes 1 or more pages to the page blob. The start and end offsets must be a multiple of 512.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-page.
     *
     * @param pageRange
     *      A {@link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param body
     *      A {@code Flowable&lt;byte[]&gt;} that contains the content of the page.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      A {@link Single &lt;RestResponse&lt;PageBlobPutPage, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobPutPageHeaders, Void>> putPages(
            PageRange pageRange, Flowable<ByteBuffer> body, BlobAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }
        if (pageRange == null) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            new IllegalArgumentException("pageRange cannot be null.");
        }
        String pageRangeStr = this.pageRangeToString(pageRange);

        return this.storageClient.pageBlobs().putPageWithRestResponseAsync(
                pageRange.end()-pageRange.start()+1, PageWriteType.UPDATE, body,
                null, pageRangeStr, accessConditions.getLeaseAccessConditions().getLeaseId(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThanOrEqual(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThan(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberEqual(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    /**
     * ClearPages frees the specified pages from the page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-page.
     *
     * @param pageRange
     *      A {@link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      A {@link Single &lt;RestResponse&lt;PageBlobPutPage, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobPutPageHeaders, Void>> clearPages(
            PageRange pageRange, BlobAccessConditions accessConditions) {
     if (accessConditions == null) {
         accessConditions = BlobAccessConditions.NONE;
     }
     if (pageRange == null) {
         // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
         // subscription.
         throw new IllegalArgumentException("pageRange cannot be null.");
     }
     String pageRangeStr = this.pageRangeToString(pageRange);

     return this.storageClient.pageBlobs().putPageWithRestResponseAsync(0, PageWriteType.CLEAR,
             null,null, pageRangeStr, accessConditions.getLeaseAccessConditions().getLeaseId(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThanOrEqual(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThan(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberEqual(),
             accessConditions.getHttpAccessConditions().getIfModifiedSince(),
             accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
             accessConditions.getHttpAccessConditions().getIfMatch().toString(),
             accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    /**
     * GetPageRanges returns the list of valid page ranges for a page blob or snapshot of a page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-page-ranges.
     *
     * @param blobRange
     *      A {@link BlobRange} object specifies the range of bytes over which to list ranges, inclusively. If
     *      omitted, then all ranges for the blob are returned.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      A {@link Single &lt;RestResponse&lt;PageBlobPutPage, PageList&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobGetPageRangesHeaders, PageList>> getPageRanges(
            BlobRange blobRange, BlobAccessConditions accessConditions) {
     if(accessConditions == null) {
         accessConditions = BlobAccessConditions.NONE;
     }
     if(blobRange == null) {
         blobRange = BlobRange.DEFAULT;
     }
     return this.storageClient.pageBlobs().getPageRangesWithRestResponseAsync(null, null,
             null, blobRange.toString(), accessConditions.getLeaseAccessConditions().getLeaseId(),
             accessConditions.getHttpAccessConditions().getIfModifiedSince(),
             accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
             accessConditions.getHttpAccessConditions().getIfMatch().toString(),
             accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
             null);
    }

    /**
     * GetPageRangesDiff gets the collection of page ranges that differ between a specified snapshot and this page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-page-ranges.
     *
     * @param blobRange
     *     A {@link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param prevSnapshot
     *     A {@code org.joda.time.DateTime} specifies that the response will contain only pages that were changed
     *     between target blob and previous snapshot. Changed pages include both updated and cleared pages. The target
     *     blob may be a snapshot, as long as the snapshot specified by prevsnapshot is the older of the two.
     * @param accessConditions
     *     A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *     complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;PageBlobGetPageRangesHeaders, PageList&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobGetPageRangesHeaders, PageList>> getPageRangesDiff(
            BlobRange blobRange, String prevSnapshot, BlobAccessConditions accessConditions) {
        if(blobRange == null) {
            blobRange = BlobRange.DEFAULT;
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }
        return this.storageClient.pageBlobs().getPageRangesWithRestResponseAsync(null,null,
                prevSnapshot, blobRange.toString(), accessConditions.getLeaseAccessConditions().getLeaseId(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }

    /**
     * Resize resizes the page blob to the specified size (which must be a multiple of 512).
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/set-blob-properties.
     *
     * @param size
     *      Resizes a page blob to the specified size. If the specified value is less than the current size of the
     *      blob, then all pages above the specified value are cleared.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single &lt;RestResponse&lt;BlobSetPropertiesHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobSetPropertiesHeaders, Void>> resize(
            long size, BlobAccessConditions accessConditions) {
        if (size%512 != 0) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException("size must be a multiple of 512.");
        }
        if (accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }
        return this.storageClient.blobs().setPropertiesWithRestResponseAsync(null,
                null, null, null, null,
                null, accessConditions.getLeaseAccessConditions().getLeaseId(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null, size, null, null, null);
    }

    /**
     * SetSequenceNumber sets the page blob's sequence number.
     *
     * @param action
     *      Indicates how the service should modify the blob's sequence number.
     * @param sequenceNumber
     *      The blob's sequence number. The sequence number is a user-controlled property that you can use to track
     *      requests and manage concurrency issues.
     * @param headers
     *      A {@link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single &lt;RestResponse&lt;BlobSetPropertiesHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobSetPropertiesHeaders, Void>> setSequenceNumber(
            SequenceNumberActionType action, Long sequenceNumber, BlobHttpHeaders headers,
            BlobAccessConditions accessConditions) {
        if (sequenceNumber != null && sequenceNumber < 0) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException("SequenceNumber must be greater than or equal to 0.");
        }
        if(headers == null) {
            headers = BlobHttpHeaders.NONE;
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }
        if(action == SequenceNumberActionType.INCREMENT) {
           sequenceNumber = null;
        }
        return this.storageClient.blobs().setPropertiesWithRestResponseAsync(null,
                headers.getCacheControl(), headers.getContentType(), headers.getContentMD5(),
                headers.getContentEncoding(), headers.getContentLanguage(),
                accessConditions.getLeaseAccessConditions().getLeaseId(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                headers.getContentDisposition(),
                null, action, sequenceNumber, null);
    }

    /**
     * StartIncrementalCopy begins an operation to start an incremental copy from one page blob's snapshot to this page
     * blob. The snapshot is copied such that only the differential changes between the previously copied snapshot are
     * transferred to the destination. The copied snapshots are complete copies of the original snapshot and can be read
     * or copied from as usual. For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/incremental-copy-blob and
     * https://docs.microsoft.com/en-us/azure/virtual-machines/windows/incremental-snapshots.
     *
     * @param source
     *      A {@code java.net.URL} which specifies the name of the source page blob.
     * @param snapshot
     *      A {@code String} which specifies the snapshot on the copy source.
     * @param accessConditions
     *      A {@link BlobAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      A {@link Single &lt;RestResponse&lt;PageBlobIncrementalCopyHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobIncrementalCopyHeaders, Void>> startIncrementalCopy(
            URL source, String snapshot, BlobAccessConditions accessConditions) throws MalformedURLException {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.NONE;
        }


        UrlBuilder builder = UrlBuilder.parse(source.toString());
        builder.setQueryParameter(Constants.SNAPSHOT_QUERY_PARAMETER, snapshot);
        source = builder.toURL();

        return this.storageClient.pageBlobs().incrementalCopyWithRestResponseAsync(source.toString(),
                null, null,
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    private String pageRangeToString(PageRange pageRange) {
        if (pageRange.start() < 0 || pageRange.end() <= 0) {
            throw new IllegalArgumentException("PageRange's start and end values must be greater than or equal to " +
                    "0 if specified.");
        }
        if (pageRange.start()%512 != 0 ) {
            throw new IllegalArgumentException("PageRange's start value must be a multiple of 512.");
        }
        if (pageRange.end()%512 != 511) {
            throw new IllegalArgumentException("PageRange's end value must be 1 less than a multiple of 512.");
        }
        if (pageRange.end() <= pageRange.start()) {
            throw new IllegalArgumentException("PageRange's End value must be after the start.");
        }

        StringBuilder range = new StringBuilder("bytes=").append(pageRange.start()).append('-').append(pageRange.end());
        return range.toString();
    }
}
