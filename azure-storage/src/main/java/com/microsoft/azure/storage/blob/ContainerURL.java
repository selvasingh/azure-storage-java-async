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

import com.microsoft.azure.storage.models.*;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * Represents a URL to the Azure Storage container allowing you to manipulate its blobs.
 */
public final class ContainerURL extends StorageURL {

    public ContainerURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link ContainerURL} with the given pipeline.
     *
     * @param pipeline
     *      An {@link HttpPipeline} object to set.
     * @return
     *      A {@link ContainerURL} object with the given pipeline.
     */
    public ContainerURL withPipeline(HttpPipeline pipeline) {
        try {
            return new ContainerURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * Creates a new {@link BlockBlobURL} object by concatenating the blobName to the end of
     * ContainerURL's URL. The new BlockBlobUrl uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the BlockBlobUrl and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewBlockBlobUrl instead of calling this object's
     * NewBlockBlobUrl method.
     *
     * @param blobName
     *      A {@code String} representing the name of the blob.
     * @return
     *      A new {@link BlockBlobURL} object which references the blob with the specified name in this container.
     */
    public BlockBlobURL createBlockBlobURL(String blobName) {
        try {
            return new BlockBlobURL(super.appendToURLPath(new URL(this.storageClient.url()), blobName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * NewPageBlobURL creates a new PageBlobURL object by concatenating blobName to the end of
     * ContainerURL's URL. The new PageBlobURL uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the PageBlobURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewPageBlobURL instead of calling this object's
     * NewPageBlobURL method.
     *
     * @param blobName
     *      A {@code String} representing the name of the blob.
     * @return
     *      A new {@link PageBlobURL} object which references the blob with the specified name in this container.
     */
    public PageBlobURL createPageBlobURL(String blobName) {
        try {
            return new PageBlobURL(super.appendToURLPath(new URL(this.storageClient.url()), blobName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * NewAppendBlobURL creates a new AppendBlobURL object by concatenating blobName to the end of
     * ContainerURL's URL. The new AppendBlobURL uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the AppendBlobURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewAppendBlobURL instead of calling this object's
     * NewAppendBlobURL method.
     *
     * @param blobName
     *      A {@code String} representing the name of the blob.
     * @return
     *      A new {@link AppendBlobURL} object which references the blob with the specified name in this container.
     */
    public AppendBlobURL createAppendBlobURL(String blobName) {
        try {
            return new AppendBlobURL(super.appendToURLPath(new URL(this.storageClient.url()), blobName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * createBlobURL creates a new BlobURL object by concatenating blobName to the end of
     * ContainerURL's URL. The new BlobURL uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the BlobURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's createBlobURL instead of calling this object's
     * createBlobURL method.
     *
     * @param blobName
     *      A {@code String} representing the name of the blob.
     * @return
     *      A new {@link BlobURL} object which references the blob with the specified name in this container.
     */
    public BlobURL createBlobURL(String blobName) {
        try {
            return new BlobURL(super.appendToURLPath(new URL(this.storageClient.url()), blobName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * Create creates a new container within a storage account.
     * If a container with the same name already exists, the operation fails.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/create-container.
     *
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessType
     *      A value of the class {@link PublicAccessType}.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerCreateHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerCreateHeaders, Void>> create(
            Metadata metadata, PublicAccessType accessType) {
        if (metadata == null) {
            metadata = Metadata.NONE;
        }
        return this.storageClient.containers().createWithRestResponseAsync(
                null, metadata.toString(), accessType, null);
    }

    /**
     * Delete marks the specified container for deletion. The container and any blobs contained within it are later
     * deleted during garbage collection. For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/delete-container.
     *
     * @param accessConditions
     *      A {@link ContainerAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerDeleteHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerDeleteHeaders, Void>> delete(
            ContainerAccessConditions accessConditions) {
        if (accessConditions == null) {
            accessConditions = ContainerAccessConditions.NONE;
        }
        if (!accessConditions.getHttpAccessConditions().getIfMatch().equals(ETag.NONE) ||
                !accessConditions.getHttpAccessConditions().getIfNoneMatch().equals(ETag.NONE)) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException("ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().deleteWithRestResponseAsync(null,
                accessConditions.getLeaseID().getLeaseId(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                null);
    }

    /**
     * GetPropertiesAndMetadata returns the container's metadata and system properties.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-container-metadata.
     *
     * @param leaseAccessConditions
     *      A {@link LeaseAccessConditions} object that specifies the lease on the container if there is one.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerGetPropertiesHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerGetPropertiesHeaders, Void>> getPropertiesAndMetadata(
            LeaseAccessConditions leaseAccessConditions) {
        if (leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.NONE;
        }

        return this.storageClient.containers().getPropertiesWithRestResponseAsync(null,
                leaseAccessConditions.getLeaseId(), null);
    }

    /**
     * SetMetadata sets the container's metadata. For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/set-container-metadata.
     *
     * @param metadata
     *      A {@link Metadata} object that specifies key value pairs to set on the blob.
     * @param accessConditions
     *      A {@link ContainerAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerSetMetadataHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerSetMetadataHeaders, Void>> setMetadata(
            Metadata metadata, ContainerAccessConditions accessConditions) {
        if (accessConditions == null) {
            accessConditions = ContainerAccessConditions.NONE;
        }
        else if (accessConditions.getHttpAccessConditions().getIfMatch() != ETag.NONE ||
                accessConditions.getHttpAccessConditions().getIfNoneMatch() != ETag.NONE ||
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince() != null) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "If-Modified-Since is the only HTTP access condition supported for this API");
        }
        if (metadata == null) {
            metadata = Metadata.NONE;
        }

        return this.storageClient.containers().setMetadataWithRestResponseAsync(null,
                accessConditions.getLeaseID().getLeaseId(), metadata.toString(),
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),null);
    }

    /**
     * GetPermissions returns the container's permissions. The permissions indicate whether container's blobs may be
     * accessed publicly. For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/get-container-acl.
     *
     * @param leaseAccessConditions
     *      A {@link LeaseAccessConditions} object that specifies the lease on the container if there is one.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerGetAclHeaders, List&lt;SignedIdentifier&gt;&gt;&gt;}
     *      object if successful.
     */
    public Single<RestResponse<ContainerGetAclHeaders, List<SignedIdentifier>>> getPermissions(
            LeaseAccessConditions leaseAccessConditions) {
        if (leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.NONE;
        }

        return this.storageClient.containers().getAclWithRestResponseAsync(
                null, leaseAccessConditions.getLeaseId(), null);
    }

    /**
     * SetPermissions sets the container's permissions. The permissions indicate whether blobs in a container may be
     * accessed publicly. For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/set-container-acl.
     *
     * @param accessType
     *      A value of the class {@link PublicAccessType}. Passing null turns off public access.
     * @param identifiers
     *      A {@code java.util.List} of {@link SignedIdentifier} objects that specify the permissions for the container.
     * @param accessConditions
     *      A {@link ContainerAccessConditions} object that specifies under which conditions the operation should
     *      complete.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerSetAclHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerSetAclHeaders, Void>> setPermissions(
            PublicAccessType accessType, List<SignedIdentifier> identifiers,
            ContainerAccessConditions accessConditions) {
        if(accessConditions == null) {
            accessConditions = ContainerAccessConditions.NONE;
        }

        // TODO: validate that empty list clears permissions and null list does not change list. Document behavior.
        return this.storageClient.containers().setAclWithRestResponseAsync(identifiers, null,
                accessConditions.getLeaseID().getLeaseId(), accessType,
                accessConditions.getHttpAccessConditions().getIfModifiedSince(),
                accessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                null);
    }

    private boolean validateLeaseOperationAccessConditions(HttpAccessConditions httpAccessConditions) {
        return (httpAccessConditions.getIfMatch() == ETag.NONE &&
                httpAccessConditions.getIfNoneMatch() == ETag.NONE);
    }

    /**
     * AcquireLease acquires a lease on the container for delete operations. The lease duration must be between 15 to
     * 60 seconds, or infinite (-1). For more information, see
     * https://docs.microsoft.com/rest/api/storageservices/lease-container.
     *
     * @param proposedID
     *      A {@code String} in any valid GUID format.
     * @param duration
     *      A {@code Integer} specifies the duration of the lease, in seconds, or negative one (-1) for a lease that
     *      never expires. A non-infinite lease can be between 15 and 60 seconds.
     * @param httpAccessConditions
     *      A {@link HttpAccessConditions} object that represents HTTP access conditions.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerLeaseHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerLeaseHeaders, Void>> acquireLease(
            String proposedID, Integer duration, HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.NONE;
        }
        else if (!this.validateLeaseOperationAccessConditions(httpAccessConditions)){
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().leaseWithRestResponseAsync(LeaseActionType.ACQUIRE,
                null,null, null, duration, proposedID,
                httpAccessConditions.getIfModifiedSince(),
                httpAccessConditions.getIfUnmodifiedSince(),
                null);
    }

    /**
     * RenewLease renews the container's previously-acquired lease.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/lease-container.
     *
     * @param leaseID
     *      A {@code String} representing the lease on the blob.
     * @param httpAccessConditions
     *      A {@link HttpAccessConditions} object that represents HTTP access conditions.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobsLeaseHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerLeaseHeaders, Void>> renewLease(
            String leaseID, HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.NONE;
        }
        else if (!this.validateLeaseOperationAccessConditions(httpAccessConditions)) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().leaseWithRestResponseAsync(LeaseActionType.RENEW, null,
                leaseID, null, null, null,
                httpAccessConditions.getIfModifiedSince(),
                httpAccessConditions.getIfUnmodifiedSince(),
                null);
    }

    /**
     * ReleaseLease releases the container's previously-acquired lease.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/lease-container.
     *
     * @param leaseID
     *      A {@code String} representing the lease on the blob.
     * @param httpAccessConditions
     *      A {@link HttpAccessConditions} object that represents HTTP access conditions.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobsLeaseHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerLeaseHeaders, Void>> releaseLease(
            String leaseID, HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.NONE;
        }
        else if (!this.validateLeaseOperationAccessConditions(httpAccessConditions)) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().leaseWithRestResponseAsync(LeaseActionType.RELEASE,
                null, leaseID, null, null, null,
                httpAccessConditions.getIfModifiedSince(),
                httpAccessConditions.getIfUnmodifiedSince(),
                null);
    }

    /**
     * BreakLease breaks the container's previously-acquired lease.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/lease-container.
     *
     * @param httpAccessConditions
     *      A {@link HttpAccessConditions} object that represents HTTP access conditions.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobsLeaseHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerLeaseHeaders, Void>> breakLease(
            HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.NONE;
        }
        else if (!this.validateLeaseOperationAccessConditions(httpAccessConditions)) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().leaseWithRestResponseAsync(LeaseActionType.BREAK,
                null, null, null, null, null,
                httpAccessConditions.getIfModifiedSince(),
                httpAccessConditions.getIfUnmodifiedSince(),
                null);
    }

    /**
     * ChangeLease changes the container's leaseID.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/lease-container.
     *
     * @param leaseID
     *      A {@code String} representing the lease on the blob.
     * @param proposedID
     *      A {@code String} in any valid GUID format.
     * @param httpAccessConditions
     *      A {@link HttpAccessConditions} object that represents HTTP access conditions.
     * @return
     *      The {@link Single&lt;RestResponse&lt;BlobsLeaseHeaders, Void&gt;&gt;} object if successful.
     */
    public Single<RestResponse<ContainerLeaseHeaders, Void>> releaseLease(
            String leaseID, String proposedID, HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.NONE;
        }
        else if (!this.validateLeaseOperationAccessConditions(httpAccessConditions)) {
            // Throwing is preferred to Single.error because this will error out immediately instead of waiting until
            // subscription.
            throw new IllegalArgumentException(
                    "ETag access conditions are not supported for this API.");
        }

        return this.storageClient.containers().leaseWithRestResponseAsync(LeaseActionType.RELEASE,
                null, leaseID, null, null, proposedID,
                httpAccessConditions.getIfModifiedSince(),
                httpAccessConditions.getIfUnmodifiedSince(),
                null);
    }

    /**
     * ListBlobs returns a single segment of blobs starting from the specified Marker. Use an empty
     * marker to start enumeration from the beginning. Blob names are returned in lexicographic order.
     * After getting a segment, process it, and then call ListBlobs again (passing the the previously-returned
     * Marker) to get the next segment.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/list-blobs.
     *
     * @param marker
     *      A {@code String} value that identifies the portion of the list to be returned with the next list operation.
     * @param options
     *      A {@link ListBlobsOptions} object which specifies one or more datasets to include in the response.
     * @return
     *      The {@link Single&lt;RestResponse&lt;ContainerListBlobsHeaders, ListBlobsResponse&gt;&gt;} object if
     *      successful.
     */
    public Single<RestResponse<ContainerListBlobsHeaders, ListBlobsResponse>> listBlobs(
            String marker, ListBlobsOptions options) {
        if (options == null) {
            options = ListBlobsOptions.DEFAULT;
        }
        return this.storageClient.containers().listBlobsWithRestResponseAsync(options.getPrefix(),
                options.getDelimiter(), marker, options.getMaxResults(),
                options.getDetails().toList(), null, null);
    }
}
