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

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Map;

import static com.microsoft.azure.storage.blob.Utility.getGMTTime;
import static com.microsoft.azure.storage.blob.Utility.getGMTTimeSnapshot;

/**
 * A BlobURLParts object represents the components that make up an Azure Storage Container/Blob URL. You parse an
 * existing URL into its parts by calling NewBlobURLParts(). You construct a URL from parts by calling URL().
 * NOTE: Changing any SAS-related field requires computing a new SAS signature.
 */
public final class BlobURLParts {
    private String scheme;

    private String host;

    private String containerName;

    private String blobName;

    private Date snapshot;

    private SASQueryParameters sasQueryParameters;

    private Map<String, String[]> unparsedParameters;

    /**
     * Creates a {@link BlobURLParts} object
     * @param scheme
     *      A {@code String} representing the scheme. Ex: "https://"
     * @param host
     *      A {@code String} representing the host. Ex: "account.blob.core.windows.net"
     * @param containerName
     *      A {@code String} representing the container name or {@code null}
     * @param blobName
     *      A {@code String} representing the blob name or {@code null}
     * @param snapshot
     *      A {@code java.util.Date} representing the snapshot time or {@code null}
     * @param sasQueryParameters
     *      A {@link SASQueryParameters} representing the SAS query parameters or {@code null}
     * @param unparsedParameters
     *      A {@code Map<String, String[]} representing query parameter vey value pairs aside from SAS parameters and
     *      snapshot time or {@code null}
     */
    public BlobURLParts(String scheme, String host, String containerName, String blobName, Date snapshot, SASQueryParameters sasQueryParameters, Map<String, String[]> unparsedParameters) {
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
     *      A {@code String} representing the scheme. Ex: "https"
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * @return
     *      A {@code String} representing the host. Ex: "account.blob.core.windows.net"
     */
    public String getHost() {
        return host;
    }

    /**
     * @return
     *      A {@code String} representing the container name or {@code null}
     */
    public String getContainerName() {
        return containerName;
    }

    /**
     * @return
     *      A {@code String} representing the blob name or {@code null}
     */
    public String getBlobName() {
        return blobName;
    }

    /**
     * @return
     *      A {@code java.util.Date} representing the snapshot time or {@code null}
     */
    public Date getSnapshot() {
        return snapshot;
    }

    /**
     * @return
     *      A {@link SASQueryParameters} representing the SAS query parameters or {@code null}
     */
    public SASQueryParameters getSasQueryParameters() {
        return sasQueryParameters;
    }

    /**
     * @return
     *      A {@code Map<String, String[]} representing query parameter vey value pairs aside from SAS parameters and
     *      snapshot time or {@code null}
     */
    public Map<String, String[]> getUnparsedParameters() {
        return unparsedParameters;
    }

    /**
     * Converts the blob URL parts to {@code String} representing a URL
     * @return
     *      A {@code String} representing a URL
     */
    public String toURL() throws UnsupportedEncodingException {
        StringBuilder urlBuilder = new StringBuilder();

        if(this.scheme != null) {
            urlBuilder.append(scheme);
            urlBuilder.append("://");
        }
        if(this.host != null) {
            urlBuilder.append(host);
        }
        if (this.containerName != null) {
            urlBuilder.append('/' + this.containerName);
            if (this.blobName != null) {
                urlBuilder.append('/' + this.blobName);
            }
        }

        boolean isFirst = true;

        for(Map.Entry<String, String[]> entry : this.unparsedParameters.entrySet()) {
            if (isFirst) {
                urlBuilder.append('?');
                isFirst = false;
            }
            else {
                urlBuilder.append('&');
            }

            urlBuilder.append(entry.getKey() + '=' + StringUtils.join(entry.getValue(), ','));
        }

        if (this.snapshot != null) {
            if (isFirst) {
                urlBuilder.append('?');
                isFirst = false;
            }
            else {
                urlBuilder.append('&');
            }

            urlBuilder.append("snapshot=" + URLEncoder.encode(getGMTTimeSnapshot(this.snapshot), "UTF-8"));
        }

        String sasEncoding = this.sasQueryParameters.encode();
        if (!Utility.isNullOrEmpty(sasEncoding)) {
            if (isFirst) {
                urlBuilder.append('?');
                isFirst = false;
            }
            else {
                urlBuilder.append('&');
            }

            urlBuilder.append(sasEncoding);
        }

        return urlBuilder.toString();
    }

    // TODO: Check that it is ok to remove final and make public setters
    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public void setBlobName(String blobName) {
        this.blobName = blobName;
    }

    public void setSnapshot(Date snapshot) {
        this.snapshot = snapshot;
    }

    public void setSasQueryParameters(SASQueryParameters sasQueryParameters) {
        this.sasQueryParameters = sasQueryParameters;
    }

    public void setUnparsedParameters(Map<String, String[]> unparsedParameters) {
        this.unparsedParameters = unparsedParameters;
    }
}
