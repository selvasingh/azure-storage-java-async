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

/**
 * AccountSAS is used to generate a Shared Access Signature (SAS) for an Azure Storage account.
 */
public final class AccountSAS extends BaseSAS {

    private final EnumSet<AccountSASService> services;

    private final EnumSet<AccountSASResourceType> resourceTypes;

    /**
     * AccountSAS is used to generate a Shared Access Signature (SAS) for an Azure Storage account.
     * @param version
     *       If null or empty, this defaults to <code>Constants.HeaderConstants.TARGET_STORAGE_VERSION</code>
     * @param protocol
     *      A {@link SASProtocol} representing the allowed Internet protocols.
     * @param startTime
     *      A <code>java.util.Date</code> object which contains the shared access signature start time.
     * @param expiryTime
     *      A <code>java.util.Date</code> object which contains the shared access signature expiry time.
     * @param permissions
     *      A <code>java.util.EnumSet</code> object that contains {@link AccountSASPermission} values that indicates
     *            the allowed permissions.
     * @param ipRange
     *      A {@link IPRange} representing the allowed IP range.
     * @param services
     *      A <code>java.util.EnumSet</code> object that contains {@link AccountSASService} values that indicates
     *            the allowed services.
     * @param resourceTypes
     *      A <code>java.util.EnumSet</code> object that contains {@link AccountSASResourceType} values that indicates
     *            the allowed resource types.
     */
    public AccountSAS(String version, SASProtocol protocol, Date startTime, Date expiryTime,
                      EnumSet<AccountSASPermission> permissions, IPRange ipRange, EnumSet<AccountSASService> services,
                      EnumSet<AccountSASResourceType> resourceTypes) {
        super(version, protocol, startTime, expiryTime, AccountSASPermission.permissionsToString(permissions), ipRange);
        this.services = services;
        this.resourceTypes = resourceTypes;
    }

    /**
     * Generates {@link SASQueryParameters} object which contains all SAS uery parameters
     * @param sharedKeyCredentials
     *      A (@link SharedKeyCredentials} object for the storage account and corresponding primary or secondary key
     * @return
     *      A {@link SASQueryParameters} object which contains all SAS uery parameters
     * @throws InvalidKeyException
     */
    @Override
    public SASQueryParameters GenerateSASQueryParameters(SharedKeyCredentials sharedKeyCredentials) throws InvalidKeyException {
        if (sharedKeyCredentials == null) {
            throw new IllegalArgumentException("SharedKeyCredentials cannot be null.");
        }

        String servicesString = AccountSASService.servicesToString(this.services);
        String resourceTypesString = AccountSASResourceType.resourceTypesToString(this.resourceTypes);
        String stringToSign = StringUtils.join(
                new String[]{
                        sharedKeyCredentials.getAccountName(),
                        super.permissions,
                        servicesString,
                        resourceTypesString,
                        Utility.getUTCTimeOrEmpty(super.startTime),
                        Utility.getUTCTimeOrEmpty(super.expiryTime),
                        super.ipRange.toString(),
                        super.protocol.toString(),
                        super.version,
                        Constants.EMPTY_STRING // Account SAS requires an additional newline character
                },
                '\n'
        );

        String signature = sharedKeyCredentials.computeHmac256(stringToSign);

        SASQueryParameters sasParams = new SASQueryParameters();
        sasParams.version = super.version;
        sasParams.resourceTypes = resourceTypesString;
        sasParams.protocol = super.protocol.toString();
        sasParams.startTime = super.startTime;
        sasParams.expiryTime = super.expiryTime;
        sasParams.ipRange = super.ipRange;
        sasParams.permissions = super.permissions;
        sasParams.signature = signature;
        return sasParams;
    }
}
