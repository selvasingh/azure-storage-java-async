/**
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.storage.samples;

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
import io.reactivex.SingleSource;
import io.reactivex.functions.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.util.Locale;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BasicSample {
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

        System.out.println("Starting an upload, cancelling using Rx.");
        // Objects representing the Azure Storage resources we're sending requests to.
        final ServiceURL serviceURL = new ServiceURL("http://javasdktest.blob.core.windows.net", pipeline);
        final ContainerURL containerURL = serviceURL.createContainerURL("javasdktest");
        final BlockBlobURL blobURL = containerURL.createBlockBlobURL("testBlob");

        // Some data to put into Azure Storage
        final byte[] data = { 0, 1, 2, 3, 4 };
        final AsyncInputStream asyncStream = AsyncInputStream.create(data);

        // Create a container.
        Single<RestResponse<BlockBlobsGetBlockListHeaders, BlockList>> finalAsyncResponse = containerURL.createAsync(null, null, null)
            .flatMap(new Function<RestResponse<ContainerCreateHeaders, Void>, Single<RestResponse<BlobsPutHeaders, Void>>>() {
            @Override
            public Single<RestResponse<BlobsPutHeaders, Void>> apply(RestResponse<ContainerCreateHeaders, Void> response) throws Exception {
                // If this method gets called, then the call was successful.
                // Responses with "error" status codes like 404 get turned into exceptions.

                // The container has been created. Let's put a blob in the container.
                return blobURL.putBlobAsync(asyncStream, null, null, null);
            }
        }).flatMap(new Function<RestResponse<BlobsPutHeaders, Void>, Single<RestResponse<BlockBlobsGetBlockListHeaders, BlockList>>>() {
            @Override
            public Single<RestResponse<BlockBlobsGetBlockListHeaders, BlockList>> apply(RestResponse<BlobsPutHeaders, Void> blobsPutHeadersVoidRestResponse) throws Exception {
                // Since we got here, the service has indicated that the blob was successfully put in the container.
                // Now let's get the block list for the blob we just put.
                return blobURL.getBlockListAsync(BlockListType.ALL, null);
            }
        });

        // None of the async methods will run their tasks until a method like .subscribe() or .blockingGet() is called.
        // If an exception was emitted in the stream, it will be thrown when blockingGet() is called.
        try {
            RestResponse<BlockBlobsGetBlockListHeaders, BlockList> finalResponse = finalAsyncResponse.blockingGet();
            if (finalResponse.body().committedBlocks() != null) {
                System.out.println("Number of committed blocks: " + finalResponse.body().committedBlocks().size());
            }
        } catch (RestException e) {
            // RestException is thrown if a service error occurs, like receiving a HTTP 400 or 500 status code.
            System.err.println("An HTTP error occurred: " + e.response().statusCode());

            // e.body() contains the deserialized XML from the service response
            if (e.body() != null) {
                // TODO: service specification should declare an exception type in order to provide type information about the deserialized XML body
                System.err.println("Here's the body: " + e.body());
            }

            // getMessage() gives a generic error message containing the response status code and body content.
            System.err.println(e.getMessage());
        }
    }
}