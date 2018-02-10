/**
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.azure.storage.blob;

import com.microsoft.rest.v2.http.UrlBuilder;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * A BlobURLParts object represents the components that make up an Azure Storage Container/Blob URL. You parse an
 * existing URL into its parts by calling NewBlobURLParts(). You construct a URL from parts by calling toURL().
 * NOTE: Changing any SAS-related field requires computing a new SAS signature.
 */
public final class BlobURLParts {
    // Fields are intentionally not final because they are mutable.

    private String scheme;

    private String host;

    private String containerName;

    private String blobName;

    private String snapshot;

    private SASQueryParameters sasQueryParameters;

    private Map<String, String[]> unparsedParameters;

    /**
     * Creates a {@link BlobURLParts} object.
     *
     * @param scheme
     *      A {@code String} representing the scheme. Ex: "https://".
     * @param host
     *      A {@code String} representing the host. Ex: "account.blob.core.windows.net".
     * @param containerName
     *      A {@code String} representing the container name or {@code null}.
     * @param blobName
     *      A {@code String} representing the blob name or {@code null}.
     * @param snapshot
     *      A {@code java.util.Date} representing the snapshot time or {@code null}.
     * @param sasQueryParameters
     *      A {@link SASQueryParameters} representing the SAS query parameters or {@code null}.
     * @param unparsedParameters
     *      A {@code Map&lt;String, String[]&gt;} representing query parameter vey value pairs aside from SAS parameters
     *      and snapshot time or {@code null}.
     */
    public BlobURLParts(String scheme, String host, String containerName, String blobName, String snapshot,
                        SASQueryParameters sasQueryParameters, Map<String, String[]> unparsedParameters) {
        this.scheme = scheme;
        this.host = host;
        this.containerName = containerName;
        this.blobName = blobName;
        this.snapshot = snapshot;
        this.sasQueryParameters = sasQueryParameters;
        this.unparsedParameters = unparsedParameters;
    }

    /**
     * @return
     *      A {@code String} representing the scheme. Ex: "https".
     */
    public String getScheme() {
        return scheme;
    }

    // TODO: docs.
    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    /**
     * @return
     *      A {@code String} representing the host. Ex: "account.blob.core.windows.net".
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return
     *      A {@code String} representing the container name or {@code null}.
     */
    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    /**
     * @return
     *      A {@code String} representing the blob name or {@code null}.
     */
    public String getBlobName() {
        return blobName;
    }

    public void setBlobName(String blobName) {
        this.blobName = blobName;
    }

    /**
     * @return
     *      A {@code java.util.Date} representing the snapshot time or {@code null}.
     */
    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    /**
     * @return
     *      A {@link SASQueryParameters} representing the SAS query parameters or {@code null}.
     */
    public SASQueryParameters getSasQueryParameters() {
        return sasQueryParameters;
    }

    public void setSasQueryParameters(SASQueryParameters sasQueryParameters) {
        this.sasQueryParameters = sasQueryParameters;
    }


    /**
     * @return
     *      A {@code Map&lt;String, String[]&gt;} representing query parameter vey value pairs aside from SAS parameters
     *      and snapshot time or {@code null}.
     */
    public Map<String, String[]> getUnparsedParameters() {
        return unparsedParameters;
    }

    // TODO: revisit
    public void setUnparsedParameters(Map<String, String[]> unparsedParameters) {
        this.unparsedParameters = unparsedParameters;
    }


    /**
     * Converts the blob URL parts to {@code String} representing a URL.
     * @return
     *      A {@code java.net.URL} to the blob resource composed of all the elements in the object.
     */
    public URL toURL() throws UnsupportedEncodingException, MalformedURLException {
        UrlBuilder url = new UrlBuilder();
        url.withScheme(this.scheme);
        url.withHost(this.host);

        StringBuilder path = new StringBuilder();
        if (this.containerName != null) {
            path.append(this.containerName);
            if (this.blobName != null) {
                path.append('/');
                path.append(this.blobName);
            }
        }
        url.withPath(path.toString());

        for (Map.Entry<String, String[]> entry : this.unparsedParameters.entrySet()) {
            url.setQueryParameter(entry.getKey(), Utility.join(entry.getValue(), ','));
        }

        if (this.snapshot != null) {
            url.setQueryParameter(Constants.SNAPSHOT_QUERY_PARAMETER, this.snapshot);
        }

        String sasString = this.sasQueryParameters.encode();
        if (sasString.length() != 0) {
            url.withQuery(sasString);
        }

        /*String query = url.query() != null && url.query().size() != 0 ?
                url.query() + this.sasQueryParameters.encode() : this.sasQueryParameters.encode();
        url.withQuery(query);*/
        return new URL(url.toString()); // TODO: replace with toURL when new autorest publishes
    }
}
