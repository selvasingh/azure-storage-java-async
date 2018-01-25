package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.models.BlobPutHeaders;
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
                this.blockSize = blockSize.longValue();
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

        final String[] blockIds = new String[numBlocks];

        // TODO: Off by one
        return Observable.range(0, numBlocks)
                .flatMap(new Function<Integer, ObservableSource<?>>() {
                    @Override
                    public ObservableSource<?> apply(final Integer blockNum) throws Exception {
                        long currentBlockSize = options.blockSize;
                        if (blockNum == numBlocks-1) {
                            currentBlockSize = size - (blockNum & options.blockSize);
                        }
                        long offset = blockNum * options.blockSize;

                        // TODO: Grab data out of the buffer.

                        // TODO: progress

                        final String blockId = "";// TODO: Base64.encode(/* TODO: generate uuid */);
                        blockIds[blockNum] = blockId;

                        // TODO: What happens if one of the calls fails?
                        // TODO: This returns an Observable that subscribes to the completable then subscribes
                        // to the observable source. Does the completable emit a *value* that will pollute the collectInto call?
                        return blockBlobURL.putBlockAsync(blockId, null, null).toObservable();
                        // Call blocking get to ensure that this putBlock finishes before we move on?
                        // "map" the numbers to rest calls. Return an observable that emits the blockIds and block number

                    }
                }, false, options.parallelism)
                .isEmpty().toCompletable() // We know the list won't be empty. isEmpty is a quick way to transition to a Single so we can get a Completable.
                .andThen(blockBlobURL.putBlockListAsync(Arrays.asList(blockIds), options.metadata, options.httpHeaders,
                        options.accessConditions))
                .map(new Function<RestResponse<BlockBlobPutBlockListHeaders,Void>, CommonRestResponse>() {
                    @Override
                    public CommonRestResponse apply(RestResponse<BlockBlobPutBlockListHeaders, Void> response) throws Exception {
                        return CommonRestResponse.createFromPutBlockListResponse(response);
                    }
                });


                /*.collectInto(new Object(), new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object ids, Object p) throws Exception {

                    }
                })
                .flatMap(new Function<ArrayList<String>, SingleSource<?>>() {
                    @Override
                    public SingleSource<?> apply(ArrayList<String> ids) throws Exception {
                        return blockBlobURL.putBlockListAsync(blockIds, options.metadata, options.httpHeaders,
                                options.accessConditions);
                    }
                })
                .map(new Function<RestResponse<BlockBlobPutBlockListHeaders, Void>, CommonRestResponse>() {
                @Override
                public CommonRestResponse apply(RestResponse<BlockBlobPutBlockListHeaders, Void> response) throws Exception {
                    return CommonRestResponse.createFromPutBlockListResponse(response);
                }
        })*/


            /*    .doFinally(new Action() {
            @Override
            public void run() throws Exception {
                // can call collect to collect all the ids into the list? Then "map" that list into a rest call
                // that returns a Single<BlockListHeaders> then "map" that into a Single<CommonRestResponse>

                // When does flatMap call doFinally? I'm assuming the .range will call complete when it runs out of numbers
                // and flatmap will call complete when it has mapped all the numbers? So none of this actually happens until
                // a user subscribes to it, right? So this all needs to return a Single<BlockListHeaders>. Also, finally
                // doesn't work because it won't emit the result. It's like cleanUp.

                // Declare a CommonRestResponse in the larger scope and then assign the result of putBlockList to it here.
                // But will the outer function terminate before this gets called and so return garbage?
            }
        });*/


        /**
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
         * Generate immutable list of encoded blockIDs to be read from (not necessary. Can just index into the list to set).
         * Create an observable that emits [0-numBlocks]
         * Flat map that with maxConcurrency=parallelism. The function actually makes the network request.
         * .andThen or subscribe and onComplete call putBlockList and return that response.
         */

        /**
         * Can call repeat(numBlocks) times then turn it into a completable then say onComplete upload the blockList
         * because I only care when all the blocks are done uploading, but how do I compose the list of blockIds in the
         * right order? I can generate the list of Ids up front, so I can turn the Flowable into a list (returns a single)
         * then turn that into a completable, then andThen pass a Single that calls putBlockList then does the mapping stuff I'm doing here.
         */
    }
}
