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
 * Access conditions specific to append blobs
 */
public final class AppendBlobAccessConditions {
    private static AppendBlobAccessConditions defaultAppendBlobAccessConditions;
    private final Integer ifAppendPositionEquals;

    private final Integer ifMaxSizeLessThanOrEqual;

    /**
     * Creates a {@link AppendBlobAccessConditions} object
     * @param ifAppendPositionEquals
     *      ensures that the AppendBlock operation succeeds only if the append position is equal to a value.
     * @param ifMaxSizeLessThanOrEqual
     *      ensures that the AppendBlock operation succeeds only if the append blob's size is less than or
     *      equal to a value.
     */
    public AppendBlobAccessConditions(Integer ifAppendPositionEquals, Integer ifMaxSizeLessThanOrEqual) {
        this.ifAppendPositionEquals = ifAppendPositionEquals;
        this.ifMaxSizeLessThanOrEqual = ifMaxSizeLessThanOrEqual;
    }

    /**
     * @return
     *      An <code>Integer</code> for ensuring that the AppendBlock operation succeeds only if the append position
     *      is equal to a value.
     */
    public Integer getIfAppendPositionEquals() {
        return ifAppendPositionEquals;
    }

    /**
     * @return
     *      An <code>Integer</code> for ensuring that the AppendBlock operation succeeds only if the append blob's size
     *      is less than or equal to a value.
     */
    public Integer getIfMaxSizeLessThanOrEqual() {
        return ifMaxSizeLessThanOrEqual;
    }

    public static AppendBlobAccessConditions getDefault() {
        if (defaultAppendBlobAccessConditions == null) {
            defaultAppendBlobAccessConditions = new AppendBlobAccessConditions(null, null);
        }

        return defaultAppendBlobAccessConditions;
    }
}
