package com.microsoft.azure.storage.blob;

public class ListContainersOptions {

    public static final ListContainersOptions DEFAULT =
            new ListContainersOptions(new ContainerListingDetails(false),null, null);

    private final ContainerListingDetails details;

    private String prefix;

    private Integer maxResults;

    /**
     * A {@link ListContainersOptions} object.
     *
     * @param details
     *      A {@link ContainerListingDetails} object indicating what additional information the service should return
     *      with each blob.
     * @param prefix
     *      A {@code String} that filters the results to return only blobs whose names begin with the specified prefix.
     * @param maxResults
     *      Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
     *      specify maxResults or specifies a value greater than 5,000, the server will return up to 5,000 items.
     */
    public ListContainersOptions(ContainerListingDetails details, String prefix, Integer maxResults) {
        if (maxResults != null && maxResults <= 0) {
            throw new IllegalArgumentException("MaxResults must be greater than 0.");
        }
        this.details = details == null ? ContainerListingDetails.NONE : details;
        this.prefix = prefix;
        this.maxResults = maxResults;
    }

    /**
     * @return
     *      A {@link ContainerListingDetails} object indicating what additional information the service should return
     *      with each blob.
     */
    public ContainerListingDetails getDetails() {
        return details;
    }

    /**
     * @return
     *      A {@code String} that filters the results to return only blobs whose names begin with the specified prefix.
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * @return
     *      Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
     *      specify maxResults or specifies a value greater than 5,000, the server will return up to 5,000 items.
     */
    public Integer getMaxResults() {
        return maxResults;
    }
}
