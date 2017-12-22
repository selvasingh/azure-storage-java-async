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
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DownloadCancellationSample {
    static HttpPipeline getPipeline() throws UnsupportedEncodingException, InvalidKeyException {
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

        SharedKeyCredentials creds = new SharedKeyCredentials("account", "key");
        TelemetryOptions telemetryOptions = new TelemetryOptions();
        PipelineOptions pop = new PipelineOptions();
        pop.telemetryOptions = telemetryOptions;
        pop.client = HttpClient.createDefault();
        pop.logger = logger;
        pop.loggingOptions = loggingOptions;
        return StorageURL.CreatePipeline(creds, pop);
    }

    public static void main(String[] args) throws Exception {
        HttpPipeline pipeline = getPipeline();

        // Get the URL of the blob we're going to upload.
        final ServiceURL serviceURL = new ServiceURL("http://accountname.blob.core.windows.net", pipeline);
        final BlockBlobURL blobURL = serviceURL.createContainerURL("accountname").createBlockBlobURL("blobname");

        System.out.println("Starting an upload, cancelling using Rx.");

        // Canceling file downloads looks a little different, because the Single<RestResponse<Headers, AsyncInputStream>> has already completed.
        // Instead, you have to dispose of the AsyncInputStream's content.
        try {
            RestResponse<BlobsGetHeaders, AsyncInputStream> response = blobURL.getBlobAsync(new BlobRange(0L, null), null, false, null).blockingGet();

            // NOTE: the AsyncInputStream returned by body() is likely to have a dispose() method added in the future.
            // So the code will instead look like: response.body().close();
            response.body().content().subscribe().dispose();
        } catch (RestException ignored) {
        }

        // You can also cancel a download by subscribing to the response content and throwing an exception in the consumer.
        try {
            blobURL.getBlobAsync(new BlobRange(0L, null), null, false, null).flatMapPublisher(new Function<RestResponse<BlobsGetHeaders, AsyncInputStream>, Publisher<byte[]>>() {
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

    }
}
