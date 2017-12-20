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
 * Represents possible services to be used for an Account SAS
 */
public enum AccountSASService {
    /**
     * Permission to access blob resources granted.
     */
    BLOB('b'),

    /**
     * Permission to access file resources granted.
     */
    FILE('f'),

    /**
     * Permission to access queue resources granted.
     */
    QUEUE('q'),

    /**
     * Permission to access table resources granted.
     */
    TABLE('t');

    char value;

    /**
     * Create a <code>AccountSASService</code>.
     *
     * @param c
     *            The <code>char</code> which represents this service.
     */
    private AccountSASService(char c) {
        this.value = c;
    }

    /**
     * Converts the given services to a {@code String}.
     *
     * @param services
     *            The services to convert to a {@code String}.
     *
     * @return A {@code String} which represents the <code>SharedAccessAccountServices</code>.
     */
    static String servicesToString(EnumSet<AccountSASService> services) {
        if (services == null) {
            return Constants.EMPTY_STRING;
        }

        StringBuilder value = new StringBuilder();

        for (AccountSASService service : services) {
            value.append(service.value);
        }

        return value.toString();
    }

    /**
     * Creates an {@link EnumSet<AccountSASService>} from the specified services string.
     *
     * @param servicesString
     *            A {@code String} which represents the <code>SharedAccessAccountServices</code>.
     * @return A {@link EnumSet<AccountSASService>} generated from the given {@code String}.
     */
    static EnumSet<AccountSASService> servicesFromString(String servicesString) {
        EnumSet<AccountSASService> services = EnumSet.noneOf(AccountSASService.class);

        for (final char c : servicesString.toLowerCase().toCharArray()) {
            boolean invalidCharacter = true;

            for (AccountSASService service : AccountSASService.values()) {
                if (c == service.value) {
                    services.add(service);
                    invalidCharacter = false;
                    break;
                }
            }

            if (invalidCharacter) {
                throw new IllegalArgumentException(
                        String.format(SR.ENUM_COULD_NOT_BE_PARSED, "Services", servicesString));
            }
        }

        return services;
    }
}
