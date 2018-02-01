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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.util.Date;
import java.util.EnumSet;

public final class ServiceSAS extends BaseSAS {

    private final String containerName;

    private final String blobName;

    private final String identifier;

    private final String cacheControl;

    private final String contentDisposition;

    private final String contentEncoding;

    private final String contentLanguage;

    private final String contentType;

    /**
     * Creates a service SAS for a container.
     *
     * @param version
     *      The version of the service this SAS will target. If not specified, it will default to the version targeted
     *      by the library.
     * @param protocol
     *      A {@link SASProtocol} object specifying which protocols may be used by this SAS.
     * @param startTime
     *      A {@code java.util.Date} specifying when the SAS will take effect.
     * @param expiryTime
     *      A {@code java.util.Date} specifying a time after which the SAS will no longer work.
     * @param permissions
     *      An {@code EnumSet&lt;ContainerSASPermission&gt;} specifying which operations the SAS user may perform.
     * @param ipRange
     *      An {@link IPRange} object specifying which IP addresses may validly use this SAS.
     * @param containerName
     *      A {@code String} specifying the name of the container the SAS user may access.
     * @param identifier
     *      A {@code String} specifying which access policy on the container this SAS references if any.
     * @param cacheControl
     *      A {@code String} specifying the control header for the SAS.
     * @param contentDisposition
     *      A {@code String} specifying the content-disposition header for the SAS.
     * @param contentEncoding
     *      A {@code String} specifying the content-encoding header for the SAS.
     * @param contentLanguage
     *      A {@code String} specifying the content-language header for the SAS.
     * @param contentType
     *      A {@code String} specifying the content-type header for the SAS.
     */
    public ServiceSAS(String version, SASProtocol protocol, Date startTime, Date expiryTime,
                      EnumSet<ContainerSASPermission> permissions,
                      IPRange ipRange, String containerName, String identifier, String cacheControl,
                      String contentDisposition, String contentEncoding, String contentLanguage, String contentType) {
        super(version, protocol, startTime, expiryTime, ContainerSASPermission.permissionsToString(permissions), ipRange);
        this.containerName = containerName;
        this.blobName = null;
        this.identifier = identifier;
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
        this.contentEncoding = contentEncoding;
        this.contentLanguage = contentLanguage;
        this.contentType = contentType;
    }

    /**
     * Creates a service SAS for a blob.
     *
     * @param version
     *      The version of the service this SAS will target. If not specified, it will default to the version targeted
     *      by the library.
     * @param protocol
     *      A {@link SASProtocol} object specifying which protocols may be used by this SAS.
     * @param startTime
     *      A {@code java.util.Date} specifying when the SAS will take effect.
     * @param expiryTime
     *      A {@code java.util.Date} specifying a time after which the SAS will no longer work.
     * @param permissions
     *      An {@code EnumSet&lt;BlobSASPermission&gt;} specifying which operations the SAS user may perform.
     * @param ipRange
     *      An {@link IPRange} object specifying which IP addresses may validly use this SAS.
     * @param containerName
     *      A {@code String} specifying the name of the container containing the blob the SAS user may access.
     * @param blobName
     *      A {@code String} specifying the name of the blob the SAS user may access.
     * @param identifier
     *      A {@code String} specifying which access policy on the container this SAS references if any.
     * @param cacheControl
     *      A {@code String} specifying the control header for the SAS.
     * @param contentDisposition
     *      A {@code String} specifying the content-disposition header for the SAS.
     * @param contentEncoding
     *      A {@code String} specifying the content-encoding header for the SAS.
     * @param contentLanguage
     *      A {@code String} specifying the content-language header for the SAS.
     * @param contentType
     *      A {@code String} specifying the content-type header for the SAS.
     */
    public ServiceSAS(String version, SASProtocol protocol, Date startTime, Date expiryTime,
                      EnumSet<BlobSASPermission> permissions,
                      IPRange ipRange, String containerName, String blobName, String identifier, String cacheControl,
                      String contentDisposition, String contentEncoding, String contentLanguage, String contentType) {
        super(version, protocol, startTime, expiryTime, BlobSASPermission.permissionsToString(permissions), ipRange);
        this.containerName = containerName;
        this.blobName = blobName;
        this.identifier = identifier;
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
        this.contentEncoding = contentEncoding;
        this.contentLanguage = contentLanguage;
        this.contentType = contentType;
    }

    /**
     * Uses an account's shared key credential to sign these signature values to produce the proper SAS query
     * parameters.
     *
     * @param sharedKeyCredentials
     *      A {@link SharedKeyCredentials} object used to sign the SAS values.
     * @return
     *      A {@link SASQueryParameters} object containing the signed query parameters.
     * @throws InvalidKeyException
     */
    @Override
    public SASQueryParameters GenerateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials)
            throws InvalidKeyException {
        if (sharedKeyCredentials == null) {
            throw new IllegalArgumentException("SharedKeyCredentials cannot be null.");
        }

        String resource = "c";
        if (Utility.isNullOrEmpty(this.blobName)) {

        }
        else {
            resource = "b";
        }

        // TODO: parse and then toString on permissions to ensure correct order. Look at the resource type to
        // use the appropriate enumSet
         String stringToSign = Utility.join(new String[]{
                        super.permissions,
                        Utility.getUTCTimeOrEmpty(super.startTime),
                        Utility.getUTCTimeOrEmpty(super.expiryTime),
                        getCanonicalName(sharedKeyCredentials.getAccountName()),
                        this.identifier,
                        super.getIPRangeAsString(),
                        super.protocol.toString(),
                        super.version,
                        this.cacheControl,
                        this.contentDisposition,
                        this.contentEncoding,
                        this.contentLanguage,
                        this.contentType
                }, '\n');

        String signature = sharedKeyCredentials.computeHmac256(stringToSign);

        SASQueryParameters sasParams = new SASQueryParameters();
        sasParams.version = super.version;
        sasParams.protocol = super.protocol.toString();
        sasParams.startTime = super.startTime;
        sasParams.expiryTime = super.expiryTime;
        sasParams.ipRange = super.ipRange;
        sasParams.identifier = this.identifier;
        sasParams.resource = resource;
        sasParams.permissions = super.permissions;
        try {
            sasParams.signature = URLEncoder.encode(signature, "UTF-8");// TODO: use non depricated version
        } catch (UnsupportedEncodingException e) {
            // If UTF-8 is not supported, we have no idea what to do
        }
        return sasParams;
    }

    private String getCanonicalName(String accountName) {
        // Container: "/blob/account/containername"
        // Blob:      "/blob/account/containername/blobname"
        StringBuilder canonicalName = new StringBuilder("/blob");
        canonicalName.append('/').append(accountName).append('/').append(this.containerName);

        if (!Utility.isNullOrEmpty(this.blobName)) {
            canonicalName.append("/").append(this.blobName.replace("\\", "/"));
        }

        return canonicalName.toString();
    }
}
