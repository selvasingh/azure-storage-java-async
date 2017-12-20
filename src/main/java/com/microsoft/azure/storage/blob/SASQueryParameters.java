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

import java.util.Date;
import java.util.Map;

/**
 * Represents the components that make up an Azure Storage SAS' query parameters.
 * <p>NOTE: Changing any field requires computing a new SAS signature using a XxxSASSignatureValues type.</p>
 */
public final class SASQueryParameters {

    /**
     * A {@code String} representing the storage version
     */
    public String version;

    /**
     * A {@code String} representing the storage services being accessed (only for Account SAS)
     */
    public String services;

    /**
     * A {@code String} representing the storage resource types being accessed (only for Account SAS)
     */
    public String resourceTypes;

    /**
     * A {@code String} representing the allowed HTTP protocol(s) or {@code null}
     */
    public String protocol;

    /**
     * A {@code java.util.Date} representing the start time for this SAS token or {@code null}
     */
    public Date startTime;

    /**
     * A {@code java.util.Date} representing the expiry time for this SAS token
     */
    public Date expiryTime;

    /**
     * A {@link IPRange} representing the range of valid IP addresses for this SAS token or {@code null}
     */
    public IPRange ipRange;

    /**
     * A {@code String} representing the signed identifier (only for Service SAS) or {@code null}
     */
    public String identifier;

    /**
     * A {@code String} representing the storage container or blob (only for Service SAS)
     */
    public String resource;

    /**
     * A {@code String} representing the storage permissions or {@code null}
     */
    public String permissions;

    /**
     * A {@code String} representing the signature for the SAS token
     */
    public String signature;

    /**
     * Creates a new {@link SASQueryParameters} object
     * @param queryParamsMap
     *      A {@code java.util.Map} representing all query parameters for the request as key-value pairs
     * @param removeSASParams
     *      When {@code true}, the SAS query parameters will be removed from queryParamsMap
     */
    public SASQueryParameters(Map<String, String[]> queryParamsMap, boolean removeSASParams) {
            String[] queryValue = queryParamsMap.get("sv");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("sv");
            }
            this.version = queryValue[0];
        }
        else {
            this.version = null;
        }

        queryValue = queryParamsMap.get("ss");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("ss");
            }
            this.services = queryValue[0];
        }
        else {
            this.services = null;
        }

        queryValue = queryParamsMap.get("srt");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("srt");
            }
            this.resourceTypes = queryValue[0];
        }
        else {
            this.resourceTypes = null;
        }

        queryValue = queryParamsMap.get("spr");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("spr");
            }
            this.protocol = queryValue[0];
        }
        else {
            this.protocol = null;
        }

        queryValue = queryParamsMap.get("st");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("st");
            }
            this.startTime = Utility.parseDate(queryValue[0]);
        }
        else {
            this.startTime = null;
        }

        queryValue = queryParamsMap.get("se");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("se");
            }
            this.expiryTime = Utility.parseDate(queryValue[0]);
        }
        else {
            this.expiryTime = null;
        }

        queryValue = queryParamsMap.get("sip");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("sip");
            }
            this.ipRange = new IPRange(queryValue[0]);
        }
        else {
            this.ipRange = null;
        }

        queryValue = queryParamsMap.get("si");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("si");
            }
            this.identifier = queryValue[0];
        }
        else {
            this.identifier = null;
        }

        queryValue = queryParamsMap.get("sr");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("sr");
            }
            this.resource = queryValue[0];
        }
        else {
            this.resource = null;
        }

        queryValue = queryParamsMap.get("sp");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("sp");
            }
            this.permissions = queryValue[0];
        }
        else {
            this.permissions = null;
        }

        queryValue = queryParamsMap.get("sig");
        if (queryValue != null) {
            if (removeSASParams) {
                queryParamsMap.remove("sig");
            }
            this.signature = queryValue[0];
        }
        else {
            this.signature = null;
        }
    }

    /**
     * Creates a new {@link SASQueryParameters} object
     */
    public SASQueryParameters() {

        this.version = version;
        this.services = services;
        this.resourceTypes = resourceTypes;
        this.protocol = protocol;
        this.startTime = startTime;
        this.expiryTime = expiryTime;
        this.ipRange = ipRange;
        this.identifier = identifier;
        this.resource = resource;
        this.permissions = permissions;
        this.signature = signature;
    }



    /**
     * Encodes all SAS query parameters into a string that can be appended to a URL
     * @return
     *  A {@code String} representing all SAS query parameters
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        if (this.version != null) {
            first = false;
            sb.append("sv=" + this.version);
        }

        if (this.services != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("ss=" + this.services);
        }

        if (this.resourceTypes != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("srt=" + this.resourceTypes);
        }

        if (this.protocol != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("spr=" + this.protocol);
        }

        if (this.startTime != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("st=" + Utility.getUTCTimeOrEmpty(this.startTime));
        }

        if (this.expiryTime != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("se=" + Utility.getUTCTimeOrEmpty(this.expiryTime));
        }

        if (this.ipRange != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("sip=" + this.ipRange.toString());
        }

        if (this.identifier != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("si=" + this.identifier);
        }

        if (this.resource != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("sr=" + this.resource);
        }

        if (this.permissions != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("sp=" + this.permissions);
        }

        if (this.signature != null) {
            if (first) {
                first = false;
            }
            else {
                sb.append('&');
            }

            sb.append("sig=" + this.signature);
        }

        return sb.toString();
    }
}
