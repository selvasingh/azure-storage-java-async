package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.models.BlobPutHeaders;
import com.microsoft.azure.storage.models.BlockBlobPutBlockHeaders;
import com.microsoft.azure.storage.models.BlockBlobPutBlockListHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import io.reactivex.*;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Function;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;

public class Highlevel {

    public class UploadToBlockBlobOptions {

        private long blockSize;

        private IProgressReceiver progressReceiver;

        private BlobHttpHeaders httpHeaders;

        private Metadata metadata;

        private BlobAccessConditions accessConditions;

        private int parallelism;

        /**
         * Creates a new object that configures the parallel upload behavior.
         *
         * @param blockSize
         *      A {@code long} that specifies the block size to use; the default (and maximum) is BlockBlobMaxPutBlock.
         *      Must be greater than 0 (null=default).
         * @param progressReceiver
         *      An object that implements the {@link IProgressReceiver} interface which will be invoked periodically as
         *      bytes are sent in a PutBlock call to the BlockBlobURL.
         * @param httpHeaders
         *      A {@link BlobHttpHeaders} to be associated with the blob when PutBlockList is called.
         * @param metadata
         *      A {@link Metadata} object to be associated with the blob when PutBlockList is called.
         * @param accessConditions
         *      A {@link BlobAccessConditions} object that indicate the access conditions for the block blob.
         * @param parallelism
         *      A {@code int} that indicates the maximum number of blocks to upload in parallel. Must be greater than 0.
         *      The default is 5 (null=default).
         */
        public UploadToBlockBlobOptions(Long blockSize, IProgressReceiver progressReceiver, BlobHttpHeaders httpHeaders,
                                        Metadata metadata, BlobAccessConditions accessConditions, Integer parallelism) {
            if (blockSize <= 0 || blockSize > Constants.MAX_BLOCK_SIZE) {
                throw new IllegalArgumentException(String.format("blockSize must be >= 0 and <= %d",
                        Constants.MAX_BLOCK_SIZE));
            }
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Parallelism must be > 0");
            }

            if (blockSize == null) {
                this.blockSize = Constants.MAX_BLOCK_SIZE;
            }
            else {
                this.blockSize = blockSize;
            }
            if (parallelism == null) {
                this.parallelism = 5;
            }
            else {
                this.parallelism = parallelism;
            }

            this.progressReceiver = progressReceiver;
            this.httpHeaders = httpHeaders;
            this.metadata = metadata;
            this.accessConditions = accessConditions;
        }
    }

    /**
     * Uploads a buffer in blocks to a block blob.
     *
     * @param data
     *      A {@code Flowable&lt;byte[]&gt;} that contains the data to upload.
     * @param size
     *      The length of the data to upload.
     * @param blockBlobURL
     *      A {@link BlockBlobURL} that points to the blob to which the data should be uploaded.
     * @param options
     *      A {@link UploadToBlockBlobOptions} object to configure the upload behavior.
     * @return
     *      A {@code Single} that will return a {@link CommonRestResponse} if successful.
     */
    public static Single<CommonRestResponse> uploadBufferToBlockBlob(
            Flowable<byte[]> data, final long size, final BlockBlobURL blockBlobURL, final UploadToBlockBlobOptions options) {
        if (size <= Constants.MAX_PUT_BLOB_BYTES) {
            // If the size can fit in 1 putBlob call, do it this way.
            if (options.progressReceiver != null) {
                // TODO: Wrap in a progress stream once progress is written.
            }

            //TODO: Get rid of AsyncInputStream
            return blockBlobURL.putBlobAsync(new AsyncInputStream(data, size, true), options.httpHeaders,
                    options.metadata, options.accessConditions)
                    .map(new Function<RestResponse<BlobPutHeaders, Void>, CommonRestResponse>() {
                        // Transform the specific RestResponse into a CommonRestResponse.
                        @Override
                        public CommonRestResponse apply(
                                RestResponse<BlobPutHeaders, Void> response) throws Exception {
                            return CommonRestResponse.createFromPutBlobResponse(response);
                        }
                    });
        }

        final int numBlocks = (int) (((size-1) / options.blockSize) + 1);
        if (numBlocks > Constants.MAX_BLOCKS) {
            throw new IllegalArgumentException(String.format(
                    "The streamSize is too big or the blockSize is too small; the number of blocks must be <= %d",
                    Constants.MAX_BLOCKS));
        }

        // TODO: context with cancel?
        long blockSize = options.blockSize;

        // Generate a list of numbers [0-numBlocks). This determines how many times we call putBlock.
        return Observable.range(0, numBlocks)
                /*
                 For each number in the range, make a call to putBlock as follows. concatMap ensures that the items
                 emitted by this Observable are in the same sequence as they are begun, which will be important for
                 composing the list of Ids later.
                 */
                .concatMapEager(new Function<Integer, ObservableSource<String>>() {
                    @Override
                    public ObservableSource<String> apply(final Integer blockNum) throws Exception {
                        long currentBlockSize = options.blockSize;
                        // Check if we are on the last block and adjust the size accordingly.
                        if (blockNum == numBlocks-1) {
                            currentBlockSize = size - (blockNum & options.blockSize);
                        }

                        // Determine where in the original data to begin reading based on the block number.
                        long offset = blockNum * options.blockSize;

                        // TODO: Grab data out of the buffer.

                        // TODO: progress

                        final String blockId = "";// TODO: Base64.encode(/* TODO: generate uuid */);

                        // TODO: What happens if one of the calls fails?
                        /*
                         Make a call to putBlock. Instead of emitting the RestResponse, which we don't care about,
                         emit the blockId for this request. These will be collected below. Turn that into an Observable
                         which emits one item to comply with the signature of concatMapEager.
                         */
                        return blockBlobURL.putBlockAsync(blockId, null, null)
                                .map(new Function<RestResponse<BlockBlobPutBlockHeaders,Void>, String>() {
                                    @Override
                                    public String apply(RestResponse<BlockBlobPutBlockHeaders, Void> x) throws Exception {
                                        return blockId;
                                    }
                                }).toObservable();

                /*
                 Specify the number of concurrent subscribers to this map. This determines how many concurrent rest
                 calls are made. This is so because maxConcurrency is the number of internal subscribers available to
                 subscribe to the Observables emitted by the source. A subscriber is not released for a new subscription
                 until its Observable calls onComplete, which here means that the call to putBlock is finished. Prefetch
                 is a hint that each of the Observables emitted by the source will emit only one value, which is true
                 here because we have converted from a Single.
                 */

                    }
                }, options.parallelism, 1)
                /*
                collectInto will gather each of the emitted blockIds into a list. Because we used concatMap, the Ids
                will be emitted according to their block number, which means the list generated here will be properly
                ordered. This also converts into a Single.
                */
                .collectInto(new ArrayList<String>(numBlocks), new BiConsumer<ArrayList<String>, String>() {
                    @Override
                    public void accept(ArrayList<String> ids, String id) throws Exception {
                        ids.add(id);
                    }
                })
                /*
                collectInto will not emit the list until its source calls onComplete. This means that by the time we
                call putBlock list, all of the putBlock calls will have finished. By flatMapping the list, we can
                "map" it into a call to putBlockList.
                 */
                .flatMap(new Function<ArrayList<String>, SingleSource<RestResponse<BlockBlobPutBlockListHeaders, Void>>>() {
                    public SingleSource<RestResponse<BlockBlobPutBlockListHeaders, Void>> apply(ArrayList<String> ids) throws Exception {
                        return blockBlobURL.putBlockListAsync(ids, options.metadata, options.httpHeaders,
                                options.accessConditions);
                    }
                })
                /*
                Finally, we must turn the specific response type into a CommonRestResponse by mapping.
                 */
                .map(new Function<RestResponse<BlockBlobPutBlockListHeaders, Void>, CommonRestResponse>() {
                    @Override
                    public CommonRestResponse apply(RestResponse<BlockBlobPutBlockListHeaders, Void> response) throws Exception {
                        return CommonRestResponse.createFromPutBlockListResponse(response);
                    }
                });


        /*
         * Should take in a ByteBuffer.
         * Get FileChannel from FileInputStream and call map to get MappedByteBuffer.
         * Duplicate/slice the buffer for each network call (backed by the same data)
         * Set the position and limit on the new buffer (independent per buffer object).
         * Can convert to flowable by get()-ing some relative section of the array or the whole thing. This will read
         * those bytes into memory. Create using Flowable.just(byte[]).
         *
         * TODO: Have to read from the Flowable and buffer until I get a block length or it is done. Or just take a
         * raw byte array but make sure it can work with Files primarily.
         *
         */
    }
}
