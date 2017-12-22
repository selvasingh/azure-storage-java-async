/**
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.storage.samples;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.Constants;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ICredentials;
import com.microsoft.azure.storage.blob.LoggingOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TelemetryOptions;
import com.microsoft.azure.storage.models.BlobsGetHeaders;
import com.microsoft.azure.storage.models.BlobsPutHeaders;
import com.microsoft.azure.storage.models.BlockBlobsGetBlockListHeaders;
import com.microsoft.azure.storage.models.BlockList;
import com.microsoft.azure.storage.models.BlockListType;
import com.microsoft.azure.storage.models.ContainerCreateHeaders;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.AsyncInputStream;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.SingleSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BasicSample {
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
        // TODO: link to Rx primer
        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT_NAME");
        String accountKey = System.getenv("AZURE_STORAGE_ACCOUNT_KEY");

        HttpPipeline pipeline = getPipeline(accountName, accountKey);

        // Objects representing the Azure Storage resources we're sending requests to.
        final ServiceURL serviceURL = new ServiceURL("http://" + accountName + ".blob.core.windows.net", pipeline);
        final ContainerURL containerURL = serviceURL.createContainerURL("javasdktest");
        final BlockBlobURL blobURL = containerURL.createBlockBlobURL("testBlob");

        // Some data to put into Azure Storage
        final byte[] data = { 0, 1, 2, 3, 4 };

        // Convert the data to the common interface used for streaming transfers.
        final AsyncInputStream asyncStream = AsyncInputStream.create(data);
        // Create a container.
        containerURL.createAsync(null, null)
                .flatMap(new Function<RestResponse<ContainerCreateHeaders, Void>, Single<RestResponse<BlobsPutHeaders, Void>>>() {
                    @Override
                    public Single<RestResponse<BlobsPutHeaders, Void>> apply(RestResponse<ContainerCreateHeaders, Void> response) throws Exception {
                        // This method is called when the container was created successfully.

                        // The container has been created. Let's put a blob in the container.
                        return blobURL.putBlobAsync(asyncStream, null, null, null);
                    }
                }).flatMap(new Function<RestResponse<BlobsPutHeaders, Void>, Single<RestResponse<BlobsGetHeaders, AsyncInputStream>>>() {
            @Override
            public Single<RestResponse<BlobsGetHeaders, AsyncInputStream>> apply(RestResponse<BlobsPutHeaders, Void> blobsPutHeadersVoidRestResponse) throws Exception {
                // This method is called after the blob is uploaded successfully.

                // Now let's download the blob.
                return blobURL.getBlobAsync(new BlobRange(0L, new Long(data.length)), null, false);
            }
        }).subscribe(new SingleObserver<RestResponse<BlobsGetHeaders, AsyncInputStream>>() {
            @Override
            public void onSubscribe(Disposable d) {
                // This is called right away, and the Disposable given can be used at any point to cancel the operation.
            }

            @Override
            public void onSuccess(RestResponse<BlobsGetHeaders, AsyncInputStream> response) {
                System.out.println("Size of the blob: " + response.headers().contentLength());
            }

            @Override
            public void onError(Throwable e) {
                System.err.println("Something went wrong along the way: " + e.getMessage());
            }
        });

        // Wait for the operation to complete.
        System.in.read();
    }
}