package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.LoggingOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TelemetryOptions;
import com.microsoft.azure.storage.models.BlobsPutHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import io.reactivex.Completable;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.schedulers.Schedulers;

import java.io.UnsupportedEncodingException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UploadCancellationSample {
    static HttpPipeline getPipeline(String accountName, String accountKey) throws UnsupportedEncodingException, InvalidKeyException {
        HttpPipelineLogger logger = new HttpPipelineLogger() {
            @Override
            public HttpPipelineLogLevel minimumLogLevel() {
                return HttpPipelineLogLevel.INFO;
            }

            @Override
            public void log(HttpPipelineLogLevel logLevel, String s, Object... objects) {
                if (logLevel == HttpPipelineLogLevel.INFO) {
                    Logger.getGlobal().info(String.format(s, objects));
                } else if (logLevel == HttpPipelineLogLevel.WARNING) {
                    Logger.getGlobal().warning(String.format(s, objects));
                } else if (logLevel == HttpPipelineLogLevel.ERROR) {
                    Logger.getGlobal().severe(String.format(s, objects));
                }
            }
        };
        LoggingOptions loggingOptions = new LoggingOptions(Level.INFO);

        SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);
        TelemetryOptions telemetryOptions = new TelemetryOptions();
        PipelineOptions pop = new PipelineOptions();
        pop.telemetryOptions = telemetryOptions;
        pop.client = HttpClient.createDefault();
        pop.logger = logger;
        pop.loggingOptions = loggingOptions;
        return StorageURL.CreatePipeline(creds, pop);
    }

    public static void main(String[] args) throws Exception {
        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT_NAME");
        String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");

        HttpPipeline pipeline = getPipeline(accountName, accountKey);

        // Get the URL of the blob we're going to upload.
        final ServiceURL serviceURL = new ServiceURL("http://" + accountName + ".blob.core.windows.net", pipeline);
        final BlockBlobURL blobURL = serviceURL.createContainerURL("javasdktest").createBlockBlobURL("blobname");

        // This shows how to upload a buffer.
        byte[] data = new byte[1024 * 1024 * 100];
        new Random().nextBytes(data);
        AsyncInputStream stream = AsyncInputStream.create(data); // Convert the byte[] to the common interface for streaming transfers.

        // Comment the above stream and uncomment this to upload a file instead.
        // This shows how to upload a file.
//        AsyncInputStream stream = AsyncInputStream.create(AsynchronousFileChannel.open(Paths.get("myfile")));

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

        // Wait for 1 second and then cancel the putBlobAsync.
        Thread.sleep(1000);
        disposable.dispose(); // dispose() will cause resources associated with the .subscribe() to be freed.

        // Delay before next example
        Thread.sleep(1000);

        System.out.println("Starting an upload, cancelling using .timeout().");
        // Use the .timeout method to cancel the upload after a certain time interval.
        blobURL.putBlobAsync(stream, null, null, null)
                .doOnDispose(new Action() {
                    @Override
                    public void run() throws Exception {
                        System.err.println("Cancelled upload using .timeout()");
                    }
                })
                .timeout(1, TimeUnit.SECONDS)
                .subscribeOn(Schedulers.io())
                .subscribe(new SingleObserver<RestResponse<BlobsPutHeaders, Void>>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                    }

                    @Override
                    public void onSuccess(RestResponse<BlobsPutHeaders, Void> response) {
                        System.err.println("Finished putBlob with .timeout() before cancellation");
                    }

                    @Override
                    public void onError(Throwable e) {
                        System.err.println("Got an exception from .timeout() of type " + e.getClass().getSimpleName());
                    }
                });

        // Delay before next example
        Thread.sleep(5000);

        System.out.println("Starting an upload, cancelling using .takeUntil().");
        // You can cancel a stream by applying .takeUntil with another stream which emits an event to indicate the original stream should be canceled.
        // The stream returned by .takeUntil() will emit CancellationException to notify the consumer that a cancellation occurred.
        blobURL.putBlobAsync(stream, null, null, null)
                .takeUntil(Completable.complete().delay(1, TimeUnit.SECONDS))
                .subscribe(new SingleObserver<RestResponse<BlobsPutHeaders, Void>>() {
                    @Override
                    public void onSubscribe(Disposable d) {
                    }

                    @Override
                    public void onSuccess(RestResponse<BlobsPutHeaders, Void> response) {
                        System.err.println("Finished putBlob with .takeUntil() before cancellation");
                    }

                    @Override
                    public void onError(Throwable e) {
                        System.err.println("Got an exception from .takeUntil() of type " + e.getClass().getSimpleName());
                    }
                });

        Thread.sleep(5000);
        System.out.println("Press any key to terminate this application.");
        System.in.read();
    }
}
