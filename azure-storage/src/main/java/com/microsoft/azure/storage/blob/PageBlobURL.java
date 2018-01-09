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
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;
import org.joda.time.DateTime;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

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
    public PageBlobURL withSnapshot(String snapshot) throws MalformedURLException, UnsupportedEncodingException {
        BlobURLParts blobURLParts = URLParser.ParseURL(super.url);
        blobURLParts.setSnapshot(snapshot);
        return new PageBlobURL(blobURLParts.toURL(), super.storageClient.httpPipeline());
    }

    /**
     * Create creates a page blob of the specified length. Call PutPage to upload data data to a page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-blob.
     * @param size
     *           Specifies the maximum size for the page blob, up to 8 TB. The page blob size must be aligned to a
     *           512-byte boundary.
     * @param sequenceNumber
     *            A user-controlled value that you can use to track requests. The value of the sequence number must be
     *            between 0 and 2^63 - 1.The default value is 0.
     * @param blobHttpHeaders
     *            A {@Link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param metadata
     *            A {@Link Metadata} object that specifies key value pairs to set on the blob.
     * @param blobAccessConditions
     *            A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *            complete.
     * @return The {@link Single &lt;RestResponse&lt;BlobsPutHeaders, Void&gt;&gt;} object if successful.
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
                new DateTime(blobAccessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(blobAccessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                blobAccessConditions.getHttpAccessConditions().getIfMatch().toString(),
                blobAccessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                size, sequenceNumber, null);
    }

    /**
     * PutPages writes 1 or more pages to the page blob. The start and end offsets must be a multiple of 512.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-page.
     * @param pageRange
     *           A {@Link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param body
     *           A {@Link AsyncInputStream} that contains the content of the page.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return A {@link Single &lt;RestResponse&lt;PageBlobsPutPage, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobsPutPageHeaders, Void>> putPagesAsync(
            PageRange pageRange, AsyncInputStream body, BlobAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.pageBlobs().putPageWithRestResponseAsync(this.url, PageWriteType.UPDATE, body,
                null, this.pageRangeToString(pageRange), accessConditions.getLeaseAccessConditions().toString(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThanOrEqual(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThan(),
                accessConditions.getPageBlobAccessConditions().getIfSequenceNumberEqual(),
                new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    /**
     * ClearPages frees the specified pages from the page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/put-page.
     * @param pageRange
     *           A {@Link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return A {@link Single &lt;RestResponse&lt;PageBlobsPutPage, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobsPutPageHeaders, Void>> clearPagesAsync(
            PageRange pageRange, BlobAccessConditions accessConditions) {
     if(accessConditions == null) {
         accessConditions = BlobAccessConditions.getDefault();
     }
     return this.storageClient.pageBlobs().putPageWithRestResponseAsync(this.url, PageWriteType.CLEAR, null,
             null, this.pageRangeToString(pageRange), accessConditions.getLeaseAccessConditions().toString(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThanOrEqual(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberLessThan(),
             accessConditions.getPageBlobAccessConditions().getIfSequenceNumberEqual(),
             new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
             new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
             accessConditions.getHttpAccessConditions().getIfMatch().toString(),
             accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    /**
     * GetPageRanges returns the list of valid page ranges for a page blob or snapshot of a page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-page-ranges.
     * @param blobRange
     *           A {@Link BlobRange} object specifies the range of bytes over which to list ranges, inclusively. If
     *           omitted, then all ranges for the blob are returned.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return A {@link Single &lt;RestResponse&lt;PageBlobsPutPage, PageList&gt;&gt;} object if successful.
     */
    public Single<RestResponse<PageBlobsGetPageRangesHeaders, PageList>> getPageRangesAsync(
            BlobRange blobRange, BlobAccessConditions accessConditions) {
     if(accessConditions == null) {
         accessConditions = BlobAccessConditions.getDefault();
     }
     if(blobRange == null) {
         blobRange.getDefault();
     }
     return this.storageClient.pageBlobs().getPageRangesWithRestResponseAsync(this.url, null,
             null, null,
             blobRange.toString(), accessConditions.getLeaseAccessConditions().toString(),
             new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
             new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
             accessConditions.getHttpAccessConditions().getIfMatch().toString(),
             accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
             null);
    }

    /**
     * GetPageRangesDiff gets the collection of page ranges that differ between a specified snapshot and this page blob.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-page-ranges.
     * @param blobRange
     *          A {@Link PageRange} object. Specifies the range of bytes to be written as a page.
     * @param prevSnapshot
     *          A {@Code org.joda.time.DateTime} specifies that the response will contain only pages that were changed between target blob and previous
     *          snapshot. Changed pages include both updated and cleared pages. The target blob may be a snapshot, as
     *          long as the snapshot specified by prevsnapshot is the older of the two.
     * @param accessConditions
     *          A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *          complete.
     * @return
     */
    //TODO: Get rid of joda time (use java.util.Date?)
    public Single<RestResponse<PageBlobsGetPageRangesHeaders, PageList>> getPageRangesDiffAsync(
            BlobRange blobRange, DateTime prevSnapshot, BlobAccessConditions accessConditions) {
        if(blobRange == null) {
            blobRange = BlobRange.getDefault();
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.pageBlobs().getPageRangesWithRestResponseAsync(this.url, null,
                null, prevSnapshot,
                blobRange.toString(), accessConditions.getLeaseAccessConditions().toString(),
                new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }

    /**
     * Resize resizes the page blob to the specified size (which must be a multiple of 512).
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/set-blob-properties.
     * @param length
     *            Resizes a page blob to the specified size. If the specified value is less than the current size of the
     *            blob, then all pages above the specified value are cleared.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return The {@link Single &lt;RestResponse&lt;BlobsSetPropertiesHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobsSetPropertiesHeaders, Void>> resizeAsync(
            Long length, BlobAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        return this.storageClient.blobs().setPropertiesWithRestResponseAsync(this.url, null,
                null, null, null, null,
                null, accessConditions.getLeaseAccessConditions().toString(),
                new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null, length, null, null, null);
    }

    /**
     * SetSequenceNumber sets the page blob's sequence number.
     * @param action
     *           Indicates how the service should modify the blob's sequence number.
     * @param sequenceNumber
     *           The blob's sequence number.
     *           The sequence number is a user-controlled property that you can use to track requests and manage
     *           concurrency issues.
     * @param headers
     *           A {@Link BlobHttpHeaders} object that specifies which properties to set on the blob.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return A {@link Single &lt;RestResponse&lt;BlobsSetPropertiesHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<BlobsSetPropertiesHeaders, Void>> setSequenceNumber(
            SequenceNumberActionType action, Long sequenceNumber, BlobHttpHeaders headers,
            BlobAccessConditions accessConditions) {
        if(headers == null) {
            headers = BlobHttpHeaders.getDefault();
        }
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }
        // TODO: validate sequenceNumber
        if(action == SequenceNumberActionType.INCREMENT) {
           sequenceNumber = null;
        }
        return this.storageClient.blobs().setPropertiesWithRestResponseAsync(this.url, null,
                headers.getCacheControl(), headers.getContentType(), headers.getContentMD5(),
                headers.getContentEncoding(), headers.getContentLanguage(),
                accessConditions.getLeaseAccessConditions().toString(),
                new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                headers.getContentDisposition(),
                null, action, sequenceNumber, null);
    }

    /**
     * StartIncrementalCopy begins an operation to start an incremental copy from one page blob's snapshot to this page blob.
     * The snapshot is copied such that only the differential changes between the previously copied snapshot are transferred to the destination.
     * The copied snapshots are complete copies of the original snapshot and can be read or copied from as usual.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/incremental-copy-blob and
     * https://docs.microsoft.com/en-us/azure/virtual-machines/windows/incremental-snapshots.
     * @param source
     *           A {@Code java.net.URL} which specifies the name of the source page blob.
     * @param snapshot
     *           A {@Code org.joda.time.DateTime} which specifies the snapshot on the copy source.
     * @param accessConditions
     *           A {@Link BlobAccessConditions} object that specifies under which conditions the operation should
     *           complete.
     * @return A {@link Single &lt;RestResponse&lt;PageBlobsIncrementalCopyHeaders, Void&gt;&gt;} object if successful.
     * @throws URISyntaxException
     * @throws MalformedURLException
     */
    public Single<RestResponse<PageBlobsIncrementalCopyHeaders, Void>> startIncrementalCopyAsyn(
            URL source, DateTime snapshot, BlobAccessConditions accessConditions) throws URISyntaxException, MalformedURLException {
        if(accessConditions == null) {
            accessConditions = BlobAccessConditions.getDefault();
        }

        // TODO: Should this be throwing?
        // TODO: This is broken. This needs to be fixed once formatting and encoding snapshot dateTimes is figured out.
        String query = source.getQuery();
        if(query == null) {
            query = "snapshot=" + snapshot.toString();
        }
        else {
            query += "&snapshot=" + snapshot.toString();
        }
        source = new URI(source.getProtocol(), null, source.getHost(), source.getPort(), source.getPath(),
                source.getQuery(),null).toURL();
        return this.storageClient.pageBlobs().incrementalCopyWithRestResponseAsync(this.url, source.toString(),
                null, null,
                new DateTime(accessConditions.getHttpAccessConditions().getIfModifiedSince()),
                new DateTime(accessConditions.getHttpAccessConditions().getIfUnmodifiedSince()),
                accessConditions.getHttpAccessConditions().getIfMatch().toString(),
                accessConditions.getHttpAccessConditions().getIfNoneMatch().toString(), null);
    }

    private String pageRangeToString(PageRange pageRange) {
        // TODO: Validation on PageRange.
        StringBuilder range = new StringBuilder("bytes=");
        range.append(pageRange.start());
        range.append('-');
        range.append(pageRange.end());
        return range.toString();
    }
}
