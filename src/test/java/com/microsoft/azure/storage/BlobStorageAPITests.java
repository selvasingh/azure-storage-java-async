package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.models.*;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.*;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import io.reactivex.Single;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.InvalidKeyException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertArrayEquals;


public class BlobStorageAPITests {
    /**
     * Stores a reference to the RFC1123 date/time pattern.
     */
    private static final String RFC1123_PATTERN = "EEE, dd MMM yyyy HH:mm:ss z";

    /**
     * Stores a reference to the GMT time zone.
     */
    public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

    /**
     * Stores a reference to the US locale.
     */
    public static final Locale LOCALE_US = Locale.US;
    public static DateFormat RFC1123_GMT_DATE_TIME_FORMATTER = new SimpleDateFormat(RFC1123_PATTERN, LOCALE_US);

    public static String getGMTTime() {
        return getGMTTime(new Date());
    }

    /**
     * Returns the GTM date/time String for the specified value using the RFC1123 pattern.
     *
     * @param date
     *            A <code>Date</code> object that represents the date to convert to GMT date/time in the RFC1123
     *            pattern.
     *
     * @return A {@code String} that represents the GMT date/time for the specified value using the RFC1123
     *         pattern.
     */
    public static String getGMTTime(final Date date) {
        return RFC1123_GMT_DATE_TIME_FORMATTER.format(date);
    }

    static class AddDatePolicy implements RequestPolicyFactory {

        @Override
        public RequestPolicy create(RequestPolicy next, RequestPolicyOptions options) {
            return new AddDate(next);
        }

        public final class AddDate implements RequestPolicy {
            private final DateTimeFormatter format = DateTimeFormat
                    .forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZoneUTC()
                    .withLocale(Locale.US);

            private final RequestPolicy next;
            public AddDate(RequestPolicy next) {
                this.next = next;
            }

            @Override
            public Single<HttpResponse> sendAsync(HttpRequest request) {
                request.headers().set(Constants.HeaderConstants.DATE, getGMTTime(new Date()));
                return this.next.sendAsync(request);
            }
        }
    }

    @Test
    public void testBasic() throws Exception {

        HttpPipelineLogger logger = new HttpPipelineLogger() {
            @Override
            public HttpPipelineLogLevel minimumLogLevel() {
                return HttpPipelineLogLevel.INFO;
            }

            @Override
            public void log(HttpPipelineLogLevel logLevel, String s, Object... objects) {
                if (logLevel == HttpPipelineLogLevel.INFO) {
                    Logger.getGlobal().info(String.format(s, objects));
                }
                else if (logLevel == HttpPipelineLogLevel.WARNING) {
                    Logger.getGlobal().warning(String.format(s, objects));
                }
                else if (logLevel == HttpPipelineLogLevel.ERROR) {
                    Logger.getGlobal().severe(String.format(s, objects));
                }
            }
        };

        HttpClient.Configuration configuration = new HttpClient.Configuration(
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        LoggingOptions loggingOptions = new LoggingOptions(Level.INFO);
        SharedKeyCredentials creds = new SharedKeyCredentials("account", "key");
        //AnonymousCredentials creds = new AnonymousCredentials();
        //RequestRetryFactory requestRetryFactory = new RequestRetryFactory();
        TelemetryOptions telemetryOptions = new TelemetryOptions();
        AddDatePolicy addDate = new AddDatePolicy();


//        builder.withHttpClient(HttpClient.createDefault(configuration))
//                .withLogger(logger)
//                .withRequestPolicies(requestIDFactory, telemetryFactory, addDate, creds, loggingFactory);
        //StorageClientImpl client = new StorageClientImpl(builder.build());

        PipelineOptions pop = new PipelineOptions();
        pop.logger = logger;
        pop.client = HttpClient.createDefault(configuration);
        pop.loggingOptions = loggingOptions;
        pop.telemetryOptions = telemetryOptions;
        HttpPipeline pipeline = StorageURL.CreatePipeline(creds, pop);
        ContainerURL containerURL = new ContainerURL("http://xclientdev.blob.core.windows.net/newautogencontainerr", pipeline);
        containerURL.createAsync(30, new Metadata(), PublicAccessType.BLOB).blockingGet();
        //containerURL.deleteAsync("\"http://xclientdev.blob.core.windows.net/newautogencontainer").toBlocking().value();
        //containerURL.createAsync().blockingGet();
        //containerURL.deleteAsync().blockingGet();

        final ContainerURL containerURL2 = new ContainerURL("http://xclientfileencryption.blob.core.windows.net/" + generateRandomContainerName(), pipeline);
        //containerURL.deleteAsync(null, null).blockingGet();
        //containerURL.createAsync(null, null, null).blockingGet();
        //containerURL.getPropertiesAndMetadataAsync(null, null).blockingGet().headers();

        final CountDownLatch latch = new CountDownLatch(1);
        final boolean valid = true;
        containerURL2.createAsync(null, null, null)
                .doOnError(new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) throws Exception {
                        // check if error is something other than container exists
                        //if (throwable.getCause() != null)
                        //valid = false;
                    }
                })
                .doFinally(new Action() {
                    @Override
                    public void run() throws Exception {
                        if (!valid) {
                            latch.countDown();
                            return;
                        }

                        containerURL2.deleteAsync(null, null).doFinally(
                                new Action() {
                                    @Override
                                    public void run() throws Exception {
                                        latch.countDown();
                                    }
                                }
                        );
                    }
                }).subscribe();

        latch.await();
    }

    public static String generateRandomContainerName() {
        String containerName = "container" + UUID.randomUUID().toString();
        return containerName.replace("-", "");
    }

    @Test
    public void TestPutBlobBasic() throws IOException, InvalidKeyException, InterruptedException {
        RFC1123_GMT_DATE_TIME_FORMATTER.setTimeZone(GMT_ZONE);
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
        HttpClient.Configuration configuration = new HttpClient.Configuration(
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        TelemetryOptions telemetryOptions = new TelemetryOptions();
        PipelineOptions pop = new PipelineOptions();
        pop.telemetryOptions = telemetryOptions;
        pop.client = HttpClient.createDefault(configuration);
        pop.logger = logger;
        pop.loggingOptions = loggingOptions;
        HttpPipeline pipeline = StorageURL.CreatePipeline(creds, pop);

        ServiceURL su = new ServiceURL("http://xclientdev2.blob.core.windows.net", pipeline);
        String containerName = "javatestcontainer" + System.currentTimeMillis();
        ContainerURL cu = su.createContainerURL(containerName);
        cu.createAsync(null, null, PublicAccessType.BLOB).blockingGet();
        BlockBlobURL bu = cu.createBlockBlobURL("javatestblob");
        try {
            bu.putBlobAsync(new byte[]{0, 0, 0},
                    new BlobHttpHeaders(null, null, null, null, null, null),
                    new Metadata(),
                    new BlobAccessConditions(new HttpAccessConditions(null, null, new ETag(""), new ETag("")),
                            new LeaseAccessConditions(""), null, null)).blockingGet();
            RestResponse<ServiceListContainersHeaders, ListContainersResponse> resp = su.listConatinersAsync("java", null, null,
                    null, null).blockingGet();
            List<Container> containerList = resp.body().containers();
            Assert.assertEquals(1, containerList.size());
            Assert.assertEquals(containerList.get(0).name(), containerName);
            InputStream data = bu.getBlobAsync(new BlobRange(new Long(0), new Long(3)), null, false, null).blockingGet().body();
            byte[] dataByte = new byte[3];
            data.read(dataByte, 0, 3);
            assertArrayEquals(dataByte, new byte[]{0, 0, 0});
            BlobHttpHeaders headers = new BlobHttpHeaders("myControl", "myDisposition",
                    "myContentEncoding", "myLanguage", null, "myType");
            Metadata metadata = new Metadata();
            metadata.put("foo", "bar");
            bu.setPropertiesAsync(headers, null, null).blockingGet();
            bu.setMetadaAsync(metadata, null, null).blockingGet();
            BlobsGetPropertiesHeaders receivedHeaders = bu.getPropertiesAndMetadataAsync(null, null).blockingGet().headers();
            Assert.assertEquals(headers.getCacheControl(), receivedHeaders.cacheControl());
            Assert.assertEquals(headers.getContentDisposition(), receivedHeaders.contentDisposition());
            Assert.assertEquals(headers.getContentEncoding(), receivedHeaders.contentEncoding());
            Assert.assertEquals(headers.getContentLanguage(), receivedHeaders.contentLanguage());
            Assert.assertEquals(headers.getContentType(), receivedHeaders.contentType());
            //Assert.assertEquals(metadata, receivedHeaders.metadata()); TODO: Metadata broken

            DateTime snapshot = bu.createSnapshotAsync(null, null, null).blockingGet().headers().snapshot();
            BlockBlobURL buSnapshot = bu.withSnapshot(snapshot.toDate());
            data = buSnapshot.getBlobAsync(new BlobRange(new Long(0), new Long(3)), null, false, null).blockingGet().body();
            //data.read(dataByte, 0, 3);
            //assertArrayEquals(dataByte, new byte[]{0,0,0});
            //Assert.assertEquals(headers.getContentType(), receivedHeaders.contentType()); //TODO: Snapshot parsing not working
            BlockBlobURL bu2 = cu.createBlockBlobURL("javablob2");
            bu2.startCopyAsync(bu.toString(), null, null, null, null).blockingGet();
            TimeUnit.SECONDS.sleep(5);
            receivedHeaders = bu2.getPropertiesAndMetadataAsync(null, null).blockingGet().headers();
            Assert.assertEquals(headers.getContentType(), receivedHeaders.contentType());

            BlockBlobURL bu3 = cu.createBlockBlobURL("javablob3");
            bu3.putBlockAsync("0000", new byte[]{0,0,0}, null).blockingGet();
            BlockList blockList = bu3.getBlockListAsync(BlockListType.ALL, null).blockingGet().body();
            Assert.assertEquals("0000", blockList.uncommittedBlocks().get(0).name());
            List<Blob> blobs = cu.listBlobsAsync(null,
                    new ListBlobsOptions(new BlobListingDetails(true, false, true, true),
                            null, null, null)).blockingGet().body().blobs().blob();
            Assert.assertEquals(4, blobs.size());
            ArrayList<String> blockListNames = new ArrayList<String>();
            blockListNames.add("0000");
            bu3.putBlockListAsync(blockListNames, null, null, null).blockingGet();
            data = bu3.getBlobAsync(new BlobRange(new Long(0), new Long(3)), null, false, null).blockingGet().body();
            data.read(dataByte, 0, 3);
            assertArrayEquals(dataByte, new byte[]{0,0,0});
            // TODO: SAS generation
        }
        finally {
            bu.deleteAsync(DeleteSnapshotsOptionType.INCLUDE, null, null).blockingGet();
            cu.deleteAsync(null, null).blockingGet();
        }
    }
}
