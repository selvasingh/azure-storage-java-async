package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.models.BlobPutHeaders;
import com.microsoft.azure.storage.models.BlockBlobPutBlockHeaders;
import com.microsoft.azure.storage.models.BlockBlobPutBlockListHeaders;
import com.microsoft.rest.v2.RestResponse;
import io.reactivex.*;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Function;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;

public class Highlevel {

    public static class UploadToBlockBlobOptions {

        private IProgressReceiver progressReceiver;

        private BlobHTTPHeaders httpHeaders;

        private Metadata metadata;

        private BlobAccessConditions accessConditions;

        private int parallelism;

        /**
         * Creates a new object that configures the parallel upload behavior.
         *
         * @param progressReceiver
         *      An object that implements the {@link IProgressReceiver} interface which will be invoked periodically as
         *      bytes are sent in a PutBlock call to the BlockBlobURL.
         * @param httpHeaders
         *      A {@link BlobHTTPHeaders} to be associated with the blob when PutBlockList is called.
         * @param metadata
         *      A {@link Metadata} object to be associated with the blob when PutBlockList is called.
         * @param accessConditions
         *      A {@link BlobAccessConditions} object that indicate the access conditions for the block blob.
         * @param parallelism
         *      A {@code int} that indicates the maximum number of blocks to upload in parallel. Must be greater than 0.
         *      The default is 5 (null=default).
         */
        public UploadToBlockBlobOptions(IProgressReceiver progressReceiver, BlobHTTPHeaders httpHeaders,
                                        Metadata metadata, BlobAccessConditions accessConditions, Integer parallelism) {
            if (parallelism == null) {
                this.parallelism = 5;
            }
            else if (parallelism <= 0) {
                throw new IllegalArgumentException("Parallelism must be > 0");
            } else {
                this.parallelism = parallelism;
            }

            this.progressReceiver = progressReceiver;
            this.httpHeaders = httpHeaders;
            this.metadata = metadata;
            this.accessConditions = accessConditions == null ? BlobAccessConditions.NONE : accessConditions;
        }
    }

    /**
     * Uploads an iterable of ByteBuffers to a block blob.
     *
     * @param data
     *      A {@code Iterable&lt;ByteBuffer&gt;} that contains the data to upload.
     * @param blockBlobURL
     *      A {@link BlockBlobURL} that points to the blob to which the data should be uploaded.
     * @param options
     *      A {@link UploadToBlockBlobOptions} object to configure the upload behavior.
     * @return
     *      A {@code Single} that will return a {@link CommonRestResponse} if successful.
     */
    public static Single<CommonRestResponse> uploadByteBuffersToBlockBlob(
            final Iterable<ByteBuffer> data, final BlockBlobURL blockBlobURL,
            final UploadToBlockBlobOptions options) {
        // Determine the size of the blob and the number of blocks
        long size = 0;
        int numBlocks = 0;
        for (ByteBuffer b : data) {
            size += b.remaining();
            numBlocks++;
        }

        // If the size can fit in 1 putBlob call, do it this way.
        if (numBlocks == 1 && size <= Constants.MAX_PUT_BLOB_BYTES) {
            if (options.progressReceiver != null) {
                // TODO: Wrap in a progress stream once progress is written.
            }

            ByteBuffer buf = data.iterator().next();
            return blockBlobURL.putBlob(Flowable.just(buf), buf.remaining(), options.httpHeaders,
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

        if (numBlocks > Constants.MAX_BLOCKS) {
            throw new IllegalArgumentException(SR.BLOB_OVER_MAX_BLOCK_LIMIT);
        }

        // TODO: context with cancel?

        // Generate a flowable that emits items which are the ByteBuffers in the provided Iterable.
        return Observable.fromIterable(data)
                /*
                 For each ByteBuffer, make a call to putBlock as follows. concatMap ensures that the items
                 emitted by this Observable are in the same sequence as they are begun, which will be important for
                 composing the list of Ids later.
                 */
                .concatMapEager(new Function<ByteBuffer, ObservableSource<String>>() {
                    @Override
                    public ObservableSource<String> apply(final ByteBuffer blockData) throws Exception {
                        if (blockData.remaining() > Constants.MAX_BLOCK_SIZE) {
                            throw new IllegalArgumentException(SR.INVALID_BLOCK_SIZE);
                        }

                        // TODO: progress

                        final String blockId = Base64.encode(UUID.randomUUID().toString().getBytes());

                        // TODO: What happens if one of the calls fails? It seems like this single/observable
                        // will emit an error, which will halt the collecting into a list. Will the list still
                        // be emitted or will it emit an error? In the latter, it'll just propogate. In the former,
                        // we should check the size of the blockList equals numBlocks before sending it up.

                        /*
                         Make a call to putBlock. Instead of emitting the RestResponse, which we don't care about,
                         emit the blockId for this request. These will be collected below. Turn that into an Observable
                         which emits one item to comply with the signature of concatMapEager.
                         */
                        return blockBlobURL.putBlock(blockId, Flowable.just(blockData), blockData.remaining(),
                                options.accessConditions.getLeaseAccessConditions())
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
                        return blockBlobURL.putBlockList(ids, options.metadata, options.httpHeaders,
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
         */
    }
}
