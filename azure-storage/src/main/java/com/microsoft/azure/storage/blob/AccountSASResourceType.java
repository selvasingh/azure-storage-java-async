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

import java.util.EnumSet;

/**
 * Specifies the set of possible resource types for an account shared access account policy.
 */
public final class AccountSASResourceType {
    /**
     * Permission to access service level APIs granted.
     */
    public boolean service;

    /**
     * Permission to access container level APIs (Blob Containers, Tables, Queues, File Shares) granted.
     */
    public boolean container;

    /**
     * Permission to access object level APIs (Blobs, Table Entities, Queue Messages, Files) granted.
     */
    public boolean object;

    public AccountSASResourceType() {}

    /**
     * Converts the given resource types to a {@code String}.
     *
     * @return
     *      A {@code String} which represents the {@code AccountSASResourceTypes}.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (this.service) {
            builder.append("s");
        }

        if (this.container) {
            builder.append("c");
        }

        if (this.object) {
            builder.append("o");
        }

        return builder.toString();
    }

    /**
     * Creates an {@code AccountSASResourceType} from the specified resource types string.
     *
     * @param resourceTypesString
     *      A {@code String} which represents the {@code AccountSASResourceTypes}.
     * @return
     *      A {@code AccountSASResourceType} generated from the given {@code String}.
     */
    public static AccountSASResourceType parse(String resourceTypesString) {
        AccountSASResourceType resourceType = new AccountSASResourceType();

        for (int i=0; i<resourceTypesString.length(); i++) {
            boolean invalidCharacter = true;
            char c = resourceTypesString.charAt(i);
            switch (c) {
                case 's':
                    resourceType.service = true;
                    break;
                case 'c':
                    resourceType.container = true;
                    break;
                case 'o':
                    resourceType.object = true;
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(SR.ENUM_COULD_NOT_BE_PARSED_INVALID_VALUE,
                                    "Resource Types", resourceTypesString, c));
            }
        }
        return resourceType;
    }
}