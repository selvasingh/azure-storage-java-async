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
 * Access conditions specific to leasing
 */
public final class LeaseAccessConditions {

    private static LeaseAccessConditions defaultLeaseAccessConditions;
    private final String leaseId;

    /**
     * Creates a {@link ContainerAccessConditions} object.
     * @param leaseId
     *      A {@code String} representing the lease access conditions for a container or blob
     */
    public LeaseAccessConditions(String leaseId) {
        this.leaseId = leaseId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this.leaseId == null) {
            return obj == null;
        }

        return this.leaseId.equals(obj);
    }

    @Override
    public String toString() {
        return this.leaseId;
    }

    public static LeaseAccessConditions getDefault() {
        if (defaultLeaseAccessConditions == null) {
            defaultLeaseAccessConditions = new LeaseAccessConditions(null);
        }

        return defaultLeaseAccessConditions;
    }
}
