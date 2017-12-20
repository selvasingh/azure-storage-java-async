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

public final class PageBlobAccessConditions {

    private static PageBlobAccessConditions defaultPageBlobAccessConditions;

    // IfSequenceNumberLessThan ensures that the page blob operation succeeds
    // only if the blob's sequence number is less than a value.
    // IfSequenceNumberLessThan=0 means no 'IfSequenceNumberLessThan' header specified.
    // IfSequenceNumberLessThan>0 means 'IfSequenceNumberLessThan' header specified with its value
    // IfSequenceNumberLessThan==-1 means 'IfSequenceNumberLessThan' header specified with a value of 0
    private Long ifSequenceNumberLessThan;

    // IfSequenceNumberLessThanOrEqual ensures that the page blob operation succeeds
    // only if the blob's sequence number is less than or equal to a value.
    // IfSequenceNumberLessThanOrEqual=0 means no 'IfSequenceNumberLessThanOrEqual' header specified.
    // IfSequenceNumberLessThanOrEqual>0 means 'IfSequenceNumberLessThanOrEqual' header specified with its value
    // IfSequenceNumberLessThanOrEqual=-1 means 'IfSequenceNumberLessThanOrEqual' header specified with a value of 0
    private Long ifSequenceNumberLessThanOrEqual;

    // IfSequenceNumberEqual ensures that the page blob operation succeeds
    // only if the blob's sequence number is equal to a value.
    // IfSequenceNumberEqual=0 means no 'IfSequenceNumberEqual' header specified.
    // IfSequenceNumberEqual>0 means 'IfSequenceNumberEqual' header specified with its value
    // IfSequenceNumberEqual=-1 means 'IfSequenceNumberEqual' header specified with a value of 0
    private Long ifSequenceNumberEqual;

    public PageBlobAccessConditions(Long ifSequenceNumberLessThan, Long ifSequenceNumberLessThanOrEqual, Long ifSequenceNumberEqual) {
        this.ifSequenceNumberLessThan = ifSequenceNumberLessThan;
        this.ifSequenceNumberLessThanOrEqual = ifSequenceNumberLessThanOrEqual;
        this.ifSequenceNumberEqual = ifSequenceNumberEqual;
    }

    public Long getIfSequenceNumberLessThan() {
        return ifSequenceNumberLessThan;
    }

    public Long getIfSequenceNumberLessThanOrEqual() {
        return ifSequenceNumberLessThanOrEqual;
    }

    public Long getIfSequenceNumberEqual() {
        return ifSequenceNumberEqual;
    }

    public static PageBlobAccessConditions getDefault() {
        if (defaultPageBlobAccessConditions == null) {
            defaultPageBlobAccessConditions = new PageBlobAccessConditions(null, null, null);
        }

        return defaultPageBlobAccessConditions;
    }
}
