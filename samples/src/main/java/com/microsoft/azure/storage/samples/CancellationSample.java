package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Completable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

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
        // You can cancel a stream by applying .takeUntil with another stream which indicates when to cancel.
        // The stream returned by .takeUntil() will emit CancellationException to notify the consumer that a cancellation occurred.
        try {
            blobURL.putBlobAsync(stream, null, null, null)
                    .doOnDispose(new Action() {
                        @Override
                        public void run() throws Exception {
                            // This handler runs when the stream gets disposed.
                            System.err.println("Cancelled upload using Rx");
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


        // TODO: downloads
        // takeUntil, Disposable.dispose(), throwing exception in .map()

        System.exit(0);
    }
}
