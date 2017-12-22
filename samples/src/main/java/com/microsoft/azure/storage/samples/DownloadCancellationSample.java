package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.LoggingOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TelemetryOptions;
import com.microsoft.azure.storage.models.BlobsGetHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DownloadCancellationSample {
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

        // Get the URL of the blob we're going to download.
        // The container and blob must exist already.
        final ServiceURL serviceURL = new ServiceURL("http://" + accountName + ".blob.core.windows.net", pipeline);
        final BlockBlobURL blobURL = serviceURL.createContainerURL("containername").createBlockBlobURL("blobname");

        System.out.println("Starting a download, cancelling using .dispose().");

        // Canceling file downloads looks a little different,
        // because the Single<RestResponse<Headers, AsyncInputStream>> has already completed at the time you're doing a download.
        RestResponse<BlobsGetHeaders, AsyncInputStream> response = blobURL.getBlobAsync(new BlobRange(0L, 1024L*1024L), null, false, null).blockingGet();

        // Obtain a Disposable for the response content by subscribing to it.
        Disposable disposable = response.body().content().doOnCancel(new Action() {
            @Override
            public void run() throws Exception {
                System.err.println("Canceled download using .dispose()");
            }
        }).subscribe();

        // Then dispose of the Disposable.
        disposable.dispose();

        System.out.println("Starting a download, cancelling by throwing an exception in stream consumer.");
        // You can also cancel a download by subscribing to the response content and throwing an exception in the consumer.
        blobURL.getBlobAsync(new BlobRange(0L, 1024L*1024L), null, false, null).flatMapPublisher(new Function<RestResponse<BlobsGetHeaders, AsyncInputStream>, Publisher<byte[]>>() {
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
        }, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) throws Exception {
                // throwable here is a wrapper around the exception thrown in the accept(byte[]) method.
            }
        });

        System.exit(0);
    }
}
