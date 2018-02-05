package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.models.ListContainersIncludeType;

/**
 * Details indicating what additional information the service should return with each container.
 */
public class ContainerListingDetails {

    public static final ContainerListingDetails NONE = new ContainerListingDetails(false);

    private final boolean metadata;

    /**
     * A {@link ContainerListingDetails} object.
     *
     * @param metadata
     *      A {@code boolean} indicating if metadata should be returned.
     */
    ContainerListingDetails(boolean metadata) {
        this.metadata = metadata;
    }

    /**
     * @return
     *      A {@code boolean} indicating if metadata should be returned.
     */
    public boolean getMetadata() {
        return this.metadata;
    }

    ListContainersIncludeType toIncludeType() {
        if (this.metadata) {
            return ListContainersIncludeType.METADATA;
        }
        return null;
    }
}
