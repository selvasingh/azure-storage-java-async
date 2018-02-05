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
public final class AccountSASService {
    /**
     * Permission to access blob resources granted.
     */
    public boolean blob;

    /**
     * Permission to access file resources granted.
     */
    public boolean file;

    /**
     * Permission to access queue resources granted.
     */
    public boolean queue;

    /**
     * Permission to access table resources granted.
     */
    public boolean table;

    public AccountSASService() {}

    /**
     * Converts the given services to a {@code String}.
     *
     * @return
     *      A {@code String} which represents the {@code SharedAccessAccountServices}.
     */
    @Override
    public String toString() {
        StringBuilder value = new StringBuilder();

        if (this.blob) {
            value.append('b');
        }
        if (this.file) {
            value.append('f');
        }
        if (this.queue) {
            value.append('q');
        }
        if (this.table) {
            value.append('t');
        }

        return value.toString();
    }

    /**
     * Creates an {@code AccountSASService} from the specified services string.
     *
     * @param servicesString
     *            A {@code String} which represents the {@code SharedAccessAccountServices}.
     * @return A {@code AccountSASService} generated from the given {@code String}.
     */
    public static AccountSASService parse(String servicesString) {
        AccountSASService services = new AccountSASService();

        for (int i=0; i < servicesString.length(); i++) {
            char c = servicesString.charAt(i);
            switch (c) {
                case 'b':
                    services.blob = true;
                    break;
                case 'f':
                    services.file = true;
                    break;
                case 'q':
                    services.queue = true;
                    break;
                case 't':
                    services.table = true;
                    break;
                default:
                    throw new IllegalArgumentException(
                        String.format(SR.ENUM_COULD_NOT_BE_PARSED_INVALID_VALUE, "Services", servicesString, c));
            }
        }
        return services;
    }
}
