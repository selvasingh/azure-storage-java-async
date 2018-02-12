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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.microsoft.rest.v2.DateTimeRfc1123;
import org.joda.time.DateTime;

/**
 * Defines headers for Lease operation.
 */
@JacksonXmlRootElement(localName = "Blob-Lease-Headers")
public final class BlobLeaseHeaders {
    /**
     * The ETag contains a value that you can use to perform operations
     * conditionally. If the request version is 2011-08-18 or newer, the ETag
     * value will be in quotes.
     */
    @JacksonXmlProperty(localName = "ETag")
    private String eTag;

    /**
     * Returns the date and time the container was last modified. Any operation
     * that modifies the blob, including an update of the blob's metadata or
     * properties, changes the last-modified time of the blob.
     */
    @JacksonXmlProperty(localName = "Last-Modified")
    private DateTimeRfc1123 lastModified;

    /**
     * Uniquely identifies a container's lease.
     */
    @JacksonXmlProperty(localName = "x-ms-lease-id")
    private String leaseId;

    /**
     * Approximate time remaining in the lease period, in seconds.
     */
    @JacksonXmlProperty(localName = "x-ms-lease-time")
    private Integer leaseTime;

    /**
     * This header uniquely identifies the request that was made and can be
     * used for troubleshooting the request.
     */
    @JacksonXmlProperty(localName = "x-ms-request-id")
    private String requestId;

    /**
     * Indicates the version of the Blob service used to execute the request.
     * This header is returned for requests made against version 2009-09-19 and
     * above.
     */
    @JacksonXmlProperty(localName = "x-ms-version")
    private String version;

    /**
     * UTC date/time value generated by the service that indicates the time at
     * which the response was initiated.
     */
    @JacksonXmlProperty(localName = "Date")
    private DateTimeRfc1123 dateProperty;

    /**
     * Get the eTag value.
     *
     * @return the eTag value.
     */
    public String eTag() {
        return this.eTag;
    }

    /**
     * Set the eTag value.
     *
     * @param eTag the eTag value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withETag(String eTag) {
        this.eTag = eTag;
        return this;
    }

    /**
     * Get the lastModified value.
     *
     * @return the lastModified value.
     */
    public DateTime lastModified() {
        if (this.lastModified == null) {
            return null;
        }
        return this.lastModified.dateTime();
    }

    /**
     * Set the lastModified value.
     *
     * @param lastModified the lastModified value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withLastModified(DateTime lastModified) {
        if (lastModified == null) {
            this.lastModified = null;
        } else {
            this.lastModified = new DateTimeRfc1123(lastModified);
        }
        return this;
    }

    /**
     * Get the leaseId value.
     *
     * @return the leaseId value.
     */
    public String leaseId() {
        return this.leaseId;
    }

    /**
     * Set the leaseId value.
     *
     * @param leaseId the leaseId value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withLeaseId(String leaseId) {
        this.leaseId = leaseId;
        return this;
    }

    /**
     * Get the leaseTime value.
     *
     * @return the leaseTime value.
     */
    public Integer leaseTime() {
        return this.leaseTime;
    }

    /**
     * Set the leaseTime value.
     *
     * @param leaseTime the leaseTime value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withLeaseTime(Integer leaseTime) {
        this.leaseTime = leaseTime;
        return this;
    }

    /**
     * Get the requestId value.
     *
     * @return the requestId value.
     */
    public String requestId() {
        return this.requestId;
    }

    /**
     * Set the requestId value.
     *
     * @param requestId the requestId value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    /**
     * Get the version value.
     *
     * @return the version value.
     */
    public String version() {
        return this.version;
    }

    /**
     * Set the version value.
     *
     * @param version the version value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withVersion(String version) {
        this.version = version;
        return this;
    }

    /**
     * Get the dateProperty value.
     *
     * @return the dateProperty value.
     */
    public DateTime dateProperty() {
        if (this.dateProperty == null) {
            return null;
        }
        return this.dateProperty.dateTime();
    }

    /**
     * Set the dateProperty value.
     *
     * @param dateProperty the dateProperty value to set.
     * @return the BlobLeaseHeaders object itself.
     */
    public BlobLeaseHeaders withDateProperty(DateTime dateProperty) {
        if (dateProperty == null) {
            this.dateProperty = null;
        } else {
            this.dateProperty = new DateTimeRfc1123(dateProperty);
        }
        return this;
    }
}
