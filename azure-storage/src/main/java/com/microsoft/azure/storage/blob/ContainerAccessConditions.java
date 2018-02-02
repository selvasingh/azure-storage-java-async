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

/**
 * Access conditions specific to containers
 */
public final class ContainerAccessConditions {

    public static final ContainerAccessConditions NONE = new ContainerAccessConditions(null,
            null);

    private final HttpAccessConditions httpAccessConditions;

    private final LeaseAccessConditions leaseID;

    /**
     * Creates a {@link ContainerAccessConditions} object.
     *
     * @param httpAccessConditions
     *      An {@link HttpAccessConditions} object.
     * @param leaseID
     *      A {@link LeaseAccessConditions} object.
     */
    public ContainerAccessConditions(HttpAccessConditions httpAccessConditions, LeaseAccessConditions leaseID) {
        this.httpAccessConditions = httpAccessConditions == null ? HttpAccessConditions.NONE : httpAccessConditions;
        this.leaseID = leaseID == null ? LeaseAccessConditions.NONE : leaseID;
    }

    /**
     * @return
     *      A {@link HttpAccessConditions} object
     */
    public HttpAccessConditions getHttpAccessConditions() {
        return httpAccessConditions;
    }

    /**
     * @return
     *      A {@link LeaseAccessConditions} object
     */
    public LeaseAccessConditions getLeaseID() {
        return leaseID;
    }
}
