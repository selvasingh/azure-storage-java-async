/**
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.LoggingOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TelemetryOptions;
import com.microsoft.azure.storage.models.BlobGetHeaders;
import com.microsoft.azure.storage.models.BlobPutHeaders;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;

import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import com.microsoft.rest.v2.http.HttpResponse;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BasicSample {
    private void test(){

    }
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
        // This sample depends on some knowledge of RxJava.
        // A general primer on Rx can be found at http://reactivex.io/intro.html.

        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT_NAME");
        String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");

        // Set up the HTTP pipeline.
        HttpPipeline pipeline = getPipeline(accountName, accountKey);

        // Objects representing the Azure Storage resources we're sending requests to.
        final ServiceURL serviceURL = new ServiceURL(new URL("http://" + accountName + ".blob.core.windows.net"),
                pipeline);
        final ContainerURL containerURL = serviceURL.createContainerURL("javasdktest");
        final BlockBlobURL blobURL = containerURL.createBlockBlobURL("testBlob");

        // Some data to put into Azure Storage
        final byte[] data = { 0, 1, 2, 3, 4 };

        // Convert the data to the common interface used for streaming transfers.
        final AsyncInputStream asyncStream = AsyncInputStream.create(data);

        // Comment the above stream and uncomment this to upload a file instead.
        // This shows how to upload a file.
//        AsyncInputStream stream = AsyncInputStream.create(AsynchronousFileChannel.open(Paths.get("myfile")));

        // Create a container with containerURL.createAsync().
        Disposable disposable = containerURL.createAsync(null, null)
            .toCompletable() // Converting to Completable supports error recovery from containerURL.createAsync.
            .onErrorResumeNext(new Function<Throwable, CompletableSource>() {
                @Override
                public CompletableSource apply(Throwable throwable) throws Exception {
                    // This method gets called if an error occurred when creating the container.
                    // Now we can examine the error and decide if it's recoverable.

                    // A RestException is thrown when the HTTP response has an error status such as 404.
                    if (throwable instanceof RestException) {
                        HttpResponse response = ((RestException) throwable).response();
                        if (response.statusCode() == 409) {
                            // Status code 409 means the container already exists, so we recover from the error and resume the workflow.
                            return Completable.complete();
                        }
                    }

                    // If the error wasn't due to an HTTP 409, the error is considered unrecoverable.
                    // By propagating the exception we received, the workflow completes without performing the putBlobAsync or getBlobAsync.
                    throw Exceptions.propagate(throwable);
                }
            }) // blobURL.putBlobAsync will be performed unless the container create fails with an unrecoverable error.
            .andThen(blobURL.putBlobAsync(asyncStream, null, null, null))
            .flatMap(new Function<RestResponse<BlobPutHeaders, Void>, Single<RestResponse<BlobGetHeaders, AsyncInputStream>>>() {
            @Override
            public Single<RestResponse<BlobGetHeaders, AsyncInputStream>> apply(RestResponse<BlobPutHeaders, Void> response) throws Exception {
                // This method is called after the blob is uploaded successfully.

                // Now let's download the blob.
                return blobURL.getBlobAsync(new BlobRange(0L, new Long(data.length)), null, false);
            }
            }).flatMapCompletable(new Function<RestResponse<BlobGetHeaders, AsyncInputStream>, Completable>() {
                @Override
                public Completable apply(RestResponse<BlobGetHeaders, AsyncInputStream> response) throws Exception {
                    // This method is called after getBlobAsync response headers have come back from the service.
                    // We now need to download the blob's contents.

                    // Output file path for downloaded blob.
                    final Path path = Paths.get("myFilePath");
                    final AsynchronousFileChannel outFile = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                    return response.body().content().flatMapCompletable(new Function<byte[], CompletableSource>() {
                        long position = 0;
                        @Override
                        public CompletableSource apply(byte[] bytes) throws Exception {
                            Completable result = Completable.fromFuture(outFile.write(ByteBuffer.wrap(bytes), position));
                            position += bytes.length;
                            return result;
                        }
                    })
                    // Adding a .timeout() operator here causes the file download to cancel if it hasn't completed within 1 second.
                    .timeout(1, TimeUnit.SECONDS)
                    .doOnTerminate(new Action() {
                        @Override
                        public void run() throws Exception {
                            // This will close the file if the download completed or an error occurred.
                            outFile.close();
                        }
                    });
                }
            }).subscribe(new Action() {
                @Override
                public void run() throws Exception {
                    // This method runs when the blob download is completed.
                    System.out.println("Finished blob download.");
                }
            });

        // If you want to cancel the operation, you can call .dispose() on the disposable returned by the workflow.
//         disposable.dispose();

        // Wait for the operation to complete.
        System.in.read();
    }
}