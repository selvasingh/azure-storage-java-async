package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.models.BlobsGetHeaders;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Completable;
import io.reactivex.SingleSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;

import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CancellationSample {
    public static void main(String[] args) throws Exception {
        // Some data to upload.
        byte[] data = new byte[1024 * 1024 * 10];
        new Random().nextBytes(data);

        // Convert the byte[] to the common interface for streaming transfers.
        AsyncInputStream stream = AsyncInputStream.create(data);

        // Set up the HTTP pipeline.
        HttpPipeline pipeline = HttpPipeline.build(new BasicSample.AddDatePolicyFactory());

        // Represents the blob we're going to upload.
        final BlockBlobURL blobURL = new BlockBlobURL("http://accountname.blob.core.windows.net/testContainer/testblob", pipeline);

        System.out.println("Starting an upload, cancelling using Rx.");
        try {
            blobURL.putBlobAsync(stream, null, null, null)
                    .doOnDispose(new Action() {
                        @Override
                        public void run() throws Exception {
                            System.err.println("Cancelled upload using .timeout()");
                        }
                    })
                    .timeout(1, TimeUnit.SECONDS)
                    .blockingGet();
        } catch (RuntimeException ex) {
            assert ex.getCause() instanceof TimeoutException;
        }

        // You can cancel a stream by applying .takeUntil with another stream which indicates when to cancel.
        // The stream returned by .takeUntil() will emit CancellationException to notify the consumer that a cancellation occurred.
        try {
            blobURL.putBlobAsync(stream, null, null, null)
                    .doOnDispose(new Action() {
                        @Override
                        public void run() throws Exception {
                            // This handler runs when the stream gets disposed.
                            System.err.println("Cancelled upload using .takeUntil()");
                        }
                    })
                    .takeUntil(Completable.complete().delay(2, TimeUnit.SECONDS))
                    .blockingGet();
        } catch (CancellationException ignored) {
        }

        System.out.println("Starting an upload, canceling with Disposable.dispose().");
        // Disposable allows us to cancel the operation initiated by subscribe().
        final Disposable disposable =
                blobURL.putBlobAsync(stream, null, null, null)
                        .doOnDispose(new Action() {
                            @Override
                            public void run() throws Exception {
                                System.err.println("Cancelled upload using Disposable.dispose()");
                            }
                        })
                        .subscribe();

        Thread.sleep(1000);

        // dispose() will cause resources associated with the .subscribe() to be freed.
        disposable.dispose();


        // Canceling file downloads looks a little different, because the Single<RestResponse<Headers, AsyncInputStream>> has already completed.
        // Instead, you have to dispose of the AsyncInputStream's content.
        try {
            RestResponse<BlobsGetHeaders, AsyncInputStream> response = blobURL.getBlobAsync(null, null, false, null).blockingGet();

            // NOTE: the AsyncInputStream returned by body() is likely to have a dispose() method added in the future.
            // So the code will instead look like: response.body().close();
            response.body().content().subscribe().dispose();
        } catch (RestException ignored) {
        }

        // You can also cancel a download by subscribing to the response content and throwing an exception in the consumer.
        try {
            blobURL.getBlobAsync(null, null, false, null).flatMapPublisher(new Function<RestResponse<BlobsGetHeaders, AsyncInputStream>, Publisher<byte[]>>() {
                @Override
                public Publisher<byte[]> apply(RestResponse<BlobsGetHeaders, AsyncInputStream> response) throws Exception {
                    return response.body().content();
                }
            }).doOnCancel(new Action() {
                @Override
                public void run() throws Exception {
                    System.err.println("Canceled download by throwing an exception in the onNext consumer.");
                }
            }).blockingSubscribe(new Consumer<byte[]>() {
                int count = 0;
                @Override
                public void accept(byte[] bytes) throws Exception {
                    count++;
                    if (count == 3) {
                        throw new CancellationException();
                    }
                }
            });
        } catch (CancellationException ignored) {
        }
        // takeUntil, Disposable.dispose(), throwing exception in .map()

        System.exit(0);
    }
}
