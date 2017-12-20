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
 * Options when calling delete on a base blob.
 */
public enum DeleteBlobSnapshotOptions {

    /**
     * Produces an error if the base blob has any snapshots.
     */
    NONE(""),

    /**
     * Deletes the base blob and all its snapshots.
     */
    INCLUDE("include"),

    /**
     * Deletes all the base blob's snapshots but not the base blob itself
     */
    ONLY("only");

    private final String id;

    DeleteBlobSnapshotOptions(String id) { this.id = id; }

    String getValue() { return id; }
}
