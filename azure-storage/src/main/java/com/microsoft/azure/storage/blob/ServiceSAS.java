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

    public ServiceSAS(String version, SASProtocol protocol, Date startTime, Date expiryTime, EnumSet<ContainerSASPermission> permissions,
                      IPRange ipRange, String containerName, String blobName, String identifier, String cacheControl,
                      String contentDisposition, String contentEncoding, String contentLanguage, String contentType) {
        //permissions.getClass();
        super(version, protocol, startTime, expiryTime, ContainerSASPermission.permissionsToString(permissions), ipRange);
        this.containerName = containerName;
        this.blobName = blobName;
        this.identifier = identifier;
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
        this.contentEncoding = contentEncoding;
        this.contentLanguage = contentLanguage;
        this.contentType = contentType;
    }

    @Override
    public SASQueryParameters GenerateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials) throws InvalidKeyException {
        if (sharedKeyCredentials == null) {
            throw new IllegalArgumentException("SharedKeyCredentials cannot be null.");
        }

        String resource = "c";
        if (!Utility.isNullOrEmpty(this.blobName)) {
            resource = "b";
        }

        String stringToSign = StringUtils.join(
                new String[]{
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
                },
                '\n'
        );

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
        sasParams.signature = signature;
        return sasParams;
    }

    private String getCanonicalName(String accountName) {
        // Container: "/blob/account/containername"
        // Blob:      "/blob/account/containername/blobname"
        String canoncialName = StringUtils.join(
                new String[]{
                        "/blob",
                        accountName,
                        this.containerName
                },
                '/');
        if (!Utility.isNullOrEmpty(this.blobName)) {
            canoncialName += this.blobName.replace("\\", "/");
        }

        return canoncialName;
    }
}
