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

import java.security.InvalidKeyException;
import java.util.Date;

/**
 * RESERVED FOR INTERNAL USE
 * Common class for service and account SAS
 */
abstract class BaseSAS {
    final String version;

    final SASProtocol protocol;

    final Date startTime;

    final Date expiryTime;

    final String permissions;

    final IPRange ipRange;

    BaseSAS(String version, SASProtocol protocol, Date startTime, Date expiryTime, String permissions, IPRange ipRange) {
        if (Utility.isNullOrEmpty(version)) {
            this.version = Constants.HeaderConstants.TARGET_STORAGE_VERSION;
        }
        else {
            this.version = version;
        }

        this.protocol = protocol;
        this.startTime = startTime;
        this.expiryTime = expiryTime;
        this.permissions = permissions;
        this.ipRange = ipRange;
    }

    String getIPRangeAsString() {
        String ipRangeString = Constants.EMPTY_STRING;
        if (this.ipRange != null) {
            ipRangeString = this.ipRange.toString();
        }

        return ipRangeString;
    }

    public abstract SASQueryParameters GenerateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials) throws InvalidKeyException;
}
