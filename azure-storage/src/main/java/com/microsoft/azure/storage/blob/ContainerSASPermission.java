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
 * Specifies the set of possible permissions for a container shared access policy.
 */
public enum ContainerSASPermission {
    /**
     * Specifies Read access granted.
     */
    READ('r'),

    /**
     * Specifies Add access granted.
     */
    ADD('a'),

    /**
     * Specifies Create access granted.
     */
    CREATE('c'),

    /**
     * Specifies Write access granted.
     */
    WRITE('w'),

    /**
     * Specifies Delete access granted.
     */
    DELETE('d'),

    /**
     * Specifies List access granted.
     */
    LIST('l');

    final private char value;

    /**
     * Create a {@code ContainerSASPermission}.
     *
     * @param c
     *      The {@code char} which represents this permission.
     */
    private ContainerSASPermission(char c) {
        this.value = c;
    }

    /**
     * Converts the given permissions to a {@code String}.
     *
     * @param permissions
     *      The permissions to convert to a {@code String}.
     * @return
     *      A {@code String} which represents the {@code ContainerSASPermission}.
     */
    static String toString(EnumSet<ContainerSASPermission> permissions) {
        if (permissions == null) {
            return Constants.EMPTY_STRING;
        }

        // The service supports a fixed order => racwdl
        final StringBuilder builder = new StringBuilder();

        if (permissions.contains(ContainerSASPermission.READ)) {
            builder.append("r");
        }

        if (permissions.contains(ContainerSASPermission.ADD)) {
            builder.append("a");
        }

        if (permissions.contains(ContainerSASPermission.CREATE)) {
            builder.append("c");
        }

        if (permissions.contains(ContainerSASPermission.WRITE)) {
            builder.append("w");
        }

        if (permissions.contains(ContainerSASPermission.DELETE)) {
            builder.append("d");
        }

        if (permissions.contains(ContainerSASPermission.LIST)) {
            builder.append("l");
        }

        return builder.toString();
    }

    /**
     * Creates an {@link EnumSet<ContainerSASPermission>} from the specified permissions string.
     *
     * @param permString
     *      A {@code String} which represents the {@code ContainerSASPermission}.
     * @return
     *      A {@link EnumSet<ContainerSASPermission>} generated from the given {@code String}.
     */
    public static EnumSet<ContainerSASPermission> parse(String permString) {
        EnumSet<ContainerSASPermission> permissions = EnumSet.noneOf(ContainerSASPermission.class);

        for (int i=0; i<permString.length(); i++) {
            boolean invalidCharacter = true;
            char c = permString.charAt(i);

            for (ContainerSASPermission perm : ContainerSASPermission.values()) {
                if (c == perm.value) {
                    permissions.add(perm);
                    invalidCharacter = false;
                    break;
                }
            }
            if (invalidCharacter) {
                throw new IllegalArgumentException(
                        String.format(SR.ENUM_COULD_NOT_BE_PARSED_INVALID_VALUE, "Permissions", permString, c));
            }
        }
        return permissions;
    }
}