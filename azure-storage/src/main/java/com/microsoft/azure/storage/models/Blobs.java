/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 *
 * Code generated by Microsoft (R) AutoRest Code Generator.
 * Changes may cause incorrect behavior and will be lost if the code is
 * regenerated.
 */

package com.microsoft.azure.storage.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.util.List;

/**
 * The Blobs model.
 */
@JacksonXmlRootElement(localName = "Blobs")
public final class Blobs {
    private static final class BlobPrefixWrapper {
        @JacksonXmlProperty(localName = "BlobPrefix")
        private final List<BlobPrefix> items;

        @JsonCreator
        private BlobPrefixWrapper(@JacksonXmlProperty(localName = "BlobPrefix") List<BlobPrefix> items) {
            this.items = items;
        }
    }

    /**
     * The blobPrefix property.
     */
    @JacksonXmlProperty(localName = "BlobPrefix")
    private BlobPrefixWrapper blobPrefix;

    private static final class BlobWrapper {
        @JacksonXmlProperty(localName = "Blob")
        private final List<Blob> items;

        @JsonCreator
        private BlobWrapper(@JacksonXmlProperty(localName = "Blob") List<Blob> items) {
            this.items = items;
        }
    }

    /**
     * The blob property.
     */
    @JacksonXmlProperty(localName = "Blob")
    private BlobWrapper blob;

    /**
     * Get the blobPrefix value.
     *
     * @return the blobPrefix value.
     */
    public List<BlobPrefix> blobPrefix() {
        return this.blobPrefix.items;
    }

    /**
     * Set the blobPrefix value.
     *
     * @param blobPrefix the blobPrefix value to set.
     * @return the Blobs object itself.
     */
    public Blobs withBlobPrefix(List<BlobPrefix> blobPrefix) {
        this.blobPrefix = new BlobPrefixWrapper(blobPrefix);
        return this;
    }

    /**
     * Get the blob value.
     *
     * @return the blob value.
     */
    public List<Blob> blob() {
        return this.blob.items;
    }

    /**
     * Set the blob value.
     *
     * @param blob the blob value to set.
     * @return the Blobs object itself.
     */
    public Blobs withBlob(List<Blob> blob) {
        this.blob = new BlobWrapper(blob);
        return this;
    }
}
