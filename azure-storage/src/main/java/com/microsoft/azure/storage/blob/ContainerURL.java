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

import java.util.List;

/**
 * Represents a URL to the Azure Storage container allowing you to manipulate its blobs.
 */
public final class ContainerURL extends StorageURL {

    public ContainerURL( String url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link ContainerURL} with the given pipeline.
     * @param pipeline
     *      A {@link HttpPipeline} object to set.
     * @return
     *      A {@link ContainerURL} object with the given pipeline.
     */
    public ContainerURL withPipeline(HttpPipeline pipeline) {
        return new ContainerURL(this.url, pipeline);
    }

    /**
     * Creates a new {@link BlockBlobURL} object by concatenating the blobName to the end of
     * ContainerURL's URL. The new BlockBlobUrl uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the BlockBlobUrl and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewBlockBlobUrl instead of calling this object's
     * NewBlockBlobUrl method.
     * @param blobName
     * @return
     */
    public BlockBlobURL createBlockBlobURL(String blobName) {
        return new BlockBlobURL(super.appendToURLPath(this.url, blobName), this.storageClient.httpPipeline());
    }

    /**
     * NewPageBlobURL creates a new PageBlobURL object by concatenating blobName to the end of
     * ContainerURL's URL. The new PageBlobURL uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the PageBlobURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewPageBlobURL instead of calling this object's
     * NewPageBlobURL method.
     * @param blobName
     * @return
     */
    public PageBlobURL createPageBlobURL(String blobName) {
        return new PageBlobURL(super.appendToURLPath(this.url, blobName), this.storageClient.httpPipeline());
    }

    /**
     * NewAppendBlobURL creates a new AppendBlobURL object by concatenating blobName to the end of
     * ContainerURL's URL. The new AppendBlobURL uses the same request policy pipeline as the ContainerURL.
     * To change the pipeline, create the AppendBlobURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewAppendBlobURL instead of calling this object's
     * NewAppendBlobURL method.
     * @param blobName
     * @return
     */
    public AppendBlobURL createAppendBlobURL(String blobName) {
        return new AppendBlobURL(super.appendToURLPath(this.url, blobName), this.storageClient.httpPipeline());
    }

    /**
     * Create creates a new container within a storage account.
     * If a container with the same name already exists, the operation fails.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/create-container.
     * @return
     */
    public Single<RestResponse<ContainerCreateHeaders, Void>> createAsync(
            Metadata metadata, PublicAccessType access) {
        return this.storageClient.containers().createWithRestResponseAsync(
                super.url, null, null, access, null);
    }

    /**
     * Delete marks the specified container for deletion. The container and any blobs contained within it are later deleted during garbage collection.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/delete-container.
     * @return
     */
    public Single<RestResponse<ContainerDeleteHeaders, Void>> deleteAsync(
            ContainerAccessConditions containerAccessConditions) {
        if (containerAccessConditions == null) {
            containerAccessConditions = ContainerAccessConditions.getDefault();
        }

        return this.storageClient.containers().deleteWithRestResponseAsync(super.url, null,
                containerAccessConditions.getLeaseID().toString(),
                containerAccessConditions.getHttpAccessConditions().getIfModifiedSince(),
                containerAccessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                containerAccessConditions.getHttpAccessConditions().getIfMatch().toString(),
                containerAccessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }

    /**
     * GetPropertiesAndMetadata returns the container's metadata and system properties.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/get-container-metadata.
     * @return
     */
    public Single<RestResponse<ContainerGetPropertiesHeaders, Void>> getPropertiesAndMetadataAsync(
            LeaseAccessConditions leaseAccessConditions) {
        if (leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.getDefault();
        }

        return this.storageClient.containers().getPropertiesWithRestResponseAsync(super.url, null,
                leaseAccessConditions.toString(), null);
    }

    
    public Single<RestResponse<ContainerSetMetadataHeaders, Void>> setMetadataAsync(
            String metadata, LeaseAccessConditions leaseAccessConditions,
            HttpAccessConditions httpAccessConditions) {
        if (httpAccessConditions == null) {
            httpAccessConditions = HttpAccessConditions.getDefault();
        }
        else if (httpAccessConditions.getIfMatch() != ETag.getDefault() || httpAccessConditions.getIfNoneMatch() != ETag.getDefault() ||
                httpAccessConditions.getIfUnmodifiedSince() != null) {
            throw new IllegalArgumentException("If-Modified-Since is the only HTTP access condition supported for this API");
        }

        if (leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.getDefault();
        }

        return this.storageClient.containers().setMetadataWithRestResponseAsync(url, null,
                leaseAccessConditions.toString(), metadata, httpAccessConditions.getIfModifiedSince(),null);
    }

    public Single<RestResponse<ContainerGetAclHeaders, List<SignedIdentifier>>> getPermissionsAsync(
            LeaseAccessConditions leaseAccessConditions) {
        if (leaseAccessConditions == null) {
            leaseAccessConditions = LeaseAccessConditions.getDefault();
        }

        return this.storageClient.containers().getAclWithRestResponseAsync(
                super.url, null, leaseAccessConditions.toString(), null);
    }

    public Single<RestResponse<ContainerSetAclHeaders, Void>> setPermissionsAsync(
            PublicAccessType accessType, List<SignedIdentifier> identifiers, ContainerAccessConditions containerAccessConditions) {
        if(containerAccessConditions == null) {
            containerAccessConditions = ContainerAccessConditions.getDefault();
        }
        return this.storageClient.containers().setAclWithRestResponseAsync(this.url, identifiers, null,
                containerAccessConditions.getLeaseID().toString(), accessType,
                containerAccessConditions.getHttpAccessConditions().getIfModifiedSince(),
                containerAccessConditions.getHttpAccessConditions().getIfUnmodifiedSince(),
                containerAccessConditions.getHttpAccessConditions().getIfMatch().toString(),
                containerAccessConditions.getHttpAccessConditions().getIfNoneMatch().toString(),
                null);
    }

    public Single<RestResponse<ContainerListBlobsHeaders, ListBlobsResponse>> listBlobsAsync(
            String marker, ListBlobsOptions listBlobsOptions) {
        return this.storageClient.containers().listBlobsWithRestResponseAsync(this.url, listBlobsOptions.getPrefix(),
                listBlobsOptions.getDelimiter(), marker, listBlobsOptions.getMaxResults(),
                listBlobsOptions.getDetails().toList(), null, null);
    }
}
