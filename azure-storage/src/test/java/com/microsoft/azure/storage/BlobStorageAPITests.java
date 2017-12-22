package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.models.*;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.*;
import com.microsoft.rest.v2.util.FlowableUtil;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertArrayEquals;


public class BlobStorageAPITests {

    @Test
    public void TestPutBlobBasic() throws IOException, InvalidKeyException, InterruptedException {
        /**
         * This library uses the Azure Rest Pipeline to make its requests. Details on this pipline can be found here:
         * https://github.com/Azure/azure-pipeline-go/blob/master/pipeline/doc.go All references to HttpPipeline and
         * the like refer to this structure.
         * This library uses Microsoft AutoRest to generate the protocol layer off of the Swagger API spec of the
         * blob service. All files in the implementation and models folders as well as the Interfaces in the root
         * directory are auto-generated using this tool.
         * This library's paradigm is centered around the URL object. A URL is constructed to a resource, such as
         * BlobURL. This is solely a reference to a location; the existence of a BlobURL does not indicate the existence
         * of a blob or hold any state related to the blob. The URL objects define methods for all operations related
         * to that resource (or will eventually; some are not supported in the library yet).
         * Several structures are defined on top of the auto-generated protocol layer to logically group items or
         * concepts relevant to a given operation or resource. This both reduces the length of the parameter list
         * and provides some coherency and relationship of ideas to aid the developer, improving efficiency and
         * discoverability.
         * In this sample test, we demonstrate the use of all APIs that are currently implemented. They have been tested
         * to work in these cases, but they have not been thoroughly tested. More advanced operations performed by
         * specifying or modifying calls in this test are not guaranteed to work. APIs not shown here are not guaranteed
         * to work. Any reports on bugs found will be welcomed and addressed.
         */


        // Creating a pipeline requires a credentials and a structure of pipline options to customize the behavior.
        // Credentials may be SharedKey as shown here or Anonymous as shown below.
        SharedKeyCredentials creds = new SharedKeyCredentials("account", "key");

        // Pipeline options allow for customization of the behavior of the HttpPipeline. Here we show adding a logger
        // and specifying options for logging, enabling telemetry, and enabling Fiddler. Currently, the pipeline
        // requires non-null values for all the options, though this will be changing. For now, it is best to drop in
        // these values as they are innocuous and do not affect the behavior of the request at all; they merely
        // supply additional information.
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


        // This will enable interaction with Fiddler.
        HttpClient.Configuration configuration = new HttpClient.Configuration(
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        PipelineOptions pop = new PipelineOptions();
        pop.telemetryOptions = new TelemetryOptions();
        pop.client = HttpClient.createDefault(configuration);
        pop.logger = logger;
        pop.loggingOptions = new LoggingOptions(Level.INFO);
        HttpPipeline pipeline = StorageURL.CreatePipeline(creds, pop);

        // Create a reference to the service.
        ServiceURL su = new ServiceURL("http://xclientdev2.blob.core.windows.net", pipeline);

        // Create a reference to a container. Using the ServiceURL to create the ContainerURL will propagate
        // the path to the service, appending the container name. A ContainerURL may also be created by calling the
        // constructor with a full path to the container and a pipeline.
        String containerName = "javatestcontainer" + System.currentTimeMillis();
        ContainerURL cu = su.createContainerURL(containerName);

        // Create a reference to a blob. Same pattern as containers.
        BlockBlobURL bu = cu.createBlockBlobURL("javatestblob");
        try {
            // Note: Calls to blockingGet force the call to be synchronous. This whole test is synchronous.
            // APIs will typically return a RestResponse<*HeadersType*, *BodyType*>. It is therefore possible to
            // retrieve the headers and the body of every request. If there is no body in the request, the body type
            // will be void.

            // Create the container.
            cu.createAsync(null, null, PublicAccessType.BLOB).blockingGet();

            // List the containers in the account.
            RestResponse<ServiceListContainersHeaders, ListContainersResponse> resp = su.listContainersAsync(
                    "java", null, null,null, null).blockingGet();
            List<Container> containerList = resp.body().containers();
            Assert.assertEquals(1, containerList.size());
            Assert.assertEquals(containerList.get(0).name(), containerName);

            // Create the blob with a single put. See below for the putBlock(List) scenario.
            bu.putBlobAsync(AsyncInputStream.create(new byte[]{0, 0, 0}), null, null, null).blockingGet();

            // Download the blob contents.
            AsyncInputStream data = bu.getBlobAsync(new BlobRange(new Long(0), new Long(3)),
                    null, false, null).blockingGet().body();
            byte[] dataByte = FlowableUtil.collectBytes(data.content()).blockingGet();
            assertArrayEquals(dataByte, new byte[]{0, 0, 0});

            // Set and retrieve the blob properties. Metadata is not yet supported.
            BlobHttpHeaders headers = new BlobHttpHeaders("myControl", "myDisposition",
                    "myContentEncoding", "myLanguage", null,
                    "myType");
            bu.setPropertiesAsync(headers, null, null).blockingGet();
            BlobsGetPropertiesHeaders receivedHeaders = bu.getPropertiesAndMetadataAsync(
                    null, null).blockingGet().headers();
            Assert.assertEquals(headers.getCacheControl(), receivedHeaders.cacheControl());
            Assert.assertEquals(headers.getContentDisposition(), receivedHeaders.contentDisposition());
            Assert.assertEquals(headers.getContentEncoding(), receivedHeaders.contentEncoding());
            Assert.assertEquals(headers.getContentLanguage(), receivedHeaders.contentLanguage());
            Assert.assertEquals(headers.getContentType(), receivedHeaders.contentType());

            // Create a snapshot of the blob and pull the snapshot ID out of the headers.
            String snapshot = bu.createSnapshotAsync(null, null, null).blockingGet()
                    .headers().snapshot();

            // Create a reference to the snapshot. This will return a BlockBlobURL object that references the same
            // path as the base blob with the query string including the snapshot value appended to the end.
            BlockBlobURL buSnapshot = bu.withSnapshot(snapshot);

            // Download the contents of the snapshot.
            data = buSnapshot.getBlobAsync(new BlobRange(new Long(0), new Long(3)),
                    null, false, null).blockingGet().body();
            dataByte = FlowableUtil.collectBytes(data.content()).blockingGet();
            assertArrayEquals(dataByte, new byte[]{0,0,0});

            // Create a reference to another blob and copy the first blob into this location.
            BlockBlobURL bu2 = cu.createBlockBlobURL("javablob2");
            bu2.startCopyAsync(bu.toString(), null, null, null,
                    null).blockingGet();

            // Simple delay to wait for the copy. Inefficient buf effective. A better method would be to periodically
            // poll the blob.
            TimeUnit.SECONDS.sleep(5);

            // Check the existence of the copied blob.
            receivedHeaders = bu2.getPropertiesAndMetadataAsync(null, null).blockingGet()
                    .headers();
            Assert.assertEquals(headers.getContentType(), receivedHeaders.contentType());

            // Create a reference to a new blob to upload blocks. Upload a single block.
            BlockBlobURL bu3 = cu.createBlockBlobURL("javablob3");
            bu3.putBlockAsync("0000", AsyncInputStream.create(new byte[]{0,0,0}), null).blockingGet();

            // Get the list of blocks on this blob.
            BlockList blockList = bu3.getBlockListAsync(BlockListType.ALL, null)
                    .blockingGet().body();
            Assert.assertEquals("0000", blockList.uncommittedBlocks().get(0).name());

            // Get a list of blobs in the container including copies, snapshots, and uncommitted blobs.
            List<Blob> blobs = cu.listBlobsAsync(null,
                    new ListBlobsOptions(new BlobListingDetails(
                            true, false, true, true),
                            null, null, null)).blockingGet().body().blobs().blob();
            Assert.assertEquals(4, blobs.size());

            // Commit the list of blocks. Download the blob to verify.
            ArrayList<String> blockListNames = new ArrayList<>();
            blockListNames.add("0000");
            bu3.putBlockListAsync(blockListNames, null, null, null)
                    .blockingGet();
            data = bu3.getBlobAsync(new BlobRange(new Long(0), new Long(3)),
                    null, false, null).blockingGet().body();
            dataByte = FlowableUtil.collectBytes(data.content()).blockingGet();
            assertArrayEquals(dataByte, new byte[]{0,0,0});

            // SAS -----------------------------
            // Create new anonymous credentials for the pipeline. This will do a no-op for authentication and thereby
            // not set the Authorization header as is required for SAS. We can use the same options as above.
            AnonymousCredentials creds2 = new AnonymousCredentials();
            pipeline = StorageURL.CreatePipeline(creds2, pop);

            // Create an EnumSet of permissions for the resource. This can also be done inline as shown below for
            // the service and resourceType.
            EnumSet<AccountSASPermission> permissions = EnumSet.of(
                    AccountSASPermission.READ, AccountSASPermission.WRITE);

            // Construct the AccountSAS values object. This encapsulates all the values needed to create an AccountSAS.
            AccountSAS sas = new AccountSAS("2016-05-31", SASProtocol.HTTPS_HTTP, null,
                    DateTime.now().plusDays(1).toDate(), permissions, null,
                    EnumSet.of(AccountSASService.BLOB), EnumSet.of(AccountSASResourceType.OBJECT));

            // Construct a BlobURLParts object with all the constituent pieces of a reference to the blob.
            // Use the above SAS object to generate the query parameters to pass for that parameter.
            BlobURLParts parts = new BlobURLParts("http", "xclientdev2.blob.core.windows.net",
                    containerName, "javablob", null, sas.GenerateSASQueryParameters(creds),
                    null );

            // Call toURL on the parts to get a string representation of the URL. This, along with the pipeline,
            // are used to create a new BlobURL object.
            BlockBlobURL sasBlob = new BlockBlobURL(parts.toURL(), pipeline);

            // Call putBlock and getBlockList using the SAS.
            sasBlob.putBlockAsync("0001", AsyncInputStream.create(new byte[]{1,1,1}), null).blockingGet();
            blockList = sasBlob.getBlockListAsync(BlockListType.ALL, null).blockingGet().body();
            Assert.assertEquals("0001", blockList.uncommittedBlocks().get(0).name());

            // Construct a ServiceSAS in a pattern similar to that of the AccountSAS.
            ServiceSAS serviceSAS = new ServiceSAS("2016-05-31", SASProtocol.HTTPS_HTTP,
                    DateTime.now().minusDays(1).toDate(), DateTime.now().plusDays(1).toDate(),
                    EnumSet.of(ContainerSASPermission.READ, ContainerSASPermission.WRITE),
                    null, containerName, "javablob", null, null,
                    null, null, null, null);
            parts = new BlobURLParts("http", "xclientdev2.blob.core.windows.net", containerName,
                    "javablob", null, serviceSAS.GenerateSASQueryParameters(creds),
                    null);
            BlockBlobURL serviceSasBlob = new BlockBlobURL(parts.toURL(), pipeline);
            blockList = serviceSasBlob.getBlockListAsync(BlockListType.UNCOMMITTED, null)
                    .blockingGet().body();
            Assert.assertEquals("0001", blockList.uncommittedBlocks().get(0).name());

        }
        finally {
            // Delete the blob and container.
            bu.deleteAsync(DeleteSnapshotsOptionType.INCLUDE, null, null).blockingGet();
            cu.deleteAsync(null, null).blockingGet();
        }
    }

}
