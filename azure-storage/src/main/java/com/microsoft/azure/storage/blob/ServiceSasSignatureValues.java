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

public final class ServiceSasSignatureValues {

    /**
     * The version of the service this SAS will target. If not specified, it will default to the version targeted by the
     * library.
     */
    public String version;

    /**
     * A {@link SASProtocol} value representing the allowed Internet protocols.
     */
    public SASProtocol protocol;

    /**
     * A {@code java.util.Date} specifying when the SAS will take effect.
     */
    public Date startTime;

    /**
     * A {@code java.util.Date} specifying a time after which the SAS will no longer work.
     */
    public Date expiryTime;

    /**
     * A {@code String} specifying which operations the SAS user may perform. Please refer to either
     * {@link ContainerSASPermission} or {@link BlobSASPermission} depending on the resource being accessed for help
     * constructing the permissions string.
     */
    public String permissions;

    /**
     * An {@link IPRange} object specifying which IP addresses may validly use this SAS.
     */
    public IPRange ipRange;

    /**
     * A {@code String} specifying the name of the container the SAS user may access.
     */
    public String containerName;

    /**
     * A {@code String} specifying the name of the container the SAS user may access.
     */
    public String blobName;

    /**
     * A {@code String} specifying which access policy on the container this SAS references if any.
     */
    public String identifier;

    /**
     * A {@code String} specifying the control header for the SAS.
     */
    public String cacheControl;

    /**
     * A {@code String} specifying the content-disposition header for the SAS.
     */
    public String contentDisposition;

    /**
     * A {@code String} specifying the content-encoding header for the SAS.
     *
     */
    public String contentEncoding;

    /**
     * A {@code String} specifying the content-language header for the SAS.
     */
    public String contentLanguage;

    /**
     * A {@code String} specifying the content-type header for the SAS.
     */
    public String contentType;

    public ServiceSasSignatureValues() {}

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
    public SASQueryParameters GenerateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials)
            throws InvalidKeyException {
        if (sharedKeyCredentials == null) {
            throw new IllegalArgumentException("SharedKeyCredentials cannot be null.");
        }
        if (expiryTime == null || permissions == null) {
            throw new IllegalArgumentException("ExpiryTime and Permissions cannot be null.");
        }
        if (Utility.isNullOrEmpty(version)) {
            this.version = Constants.HeaderConstants.TARGET_STORAGE_VERSION;
        }
        else {
            this.version = version;
        }

        String resource = "c";
        String verifiedPermissions;
        // calling parse and toString guarantees the proper ordering.
        if (Utility.isNullOrEmpty(this.blobName)) {
            verifiedPermissions = ContainerSASPermission.parse(this.permissions).toString();
        }
        else {
            verifiedPermissions = BlobSASPermission.parse(this.permissions).toString();
            resource = "b";
        }

        // TODO: will a null string produce an empty line?
        // use the appropriate enumSet
         String stringToSign = Utility.join(new String[]{
                        verifiedPermissions,
                        Utility.getUTCTimeOrEmpty(this.startTime),
                        Utility.getUTCTimeOrEmpty(this.expiryTime),
                        getCanonicalName(sharedKeyCredentials.getAccountName()),
                        this.identifier,
                        this.ipRange.toString(),
                        this.protocol.toString(),
                        this.version,
                        this.cacheControl,
                        this.contentDisposition,
                        this.contentEncoding,
                        this.contentLanguage,
                        this.contentType
                }, '\n');

        String signature = sharedKeyCredentials.computeHmac256(stringToSign);

        SASQueryParameters sasParams = null;
        try {
            sasParams = new SASQueryParameters(this.version, null, null,
                    this.protocol.toString(), this.startTime, this.expiryTime, this.ipRange, this.identifier, resource,
                    this.permissions, URLEncoder.encode(signature, Constants.UTF8_CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
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
