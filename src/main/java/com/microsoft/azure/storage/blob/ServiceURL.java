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

import com.microsoft.azure.storage.implementation.StorageClientImpl;
import com.microsoft.azure.storage.models.ListContainersIncludeType;
import com.microsoft.azure.storage.models.ListContainersResponse;
import com.microsoft.azure.storage.models.ServiceListContainersHeaders;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

/**
 * Represents a URL to an Azure Storage Blob Service
 */
public final class ServiceURL extends StorageURL {

    public ServiceURL(String url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    public ContainerURL createContainerURL(String containerName) {
        return new ContainerURL(super.url + "/" + containerName, this.storageClient.httpPipeline());
    }

    /**
     * ListContainers returns a single segment of containers starting from the specified Marker.
     * Use an empty marker to start enumeration from the beginning. Container names are returned in lexicographic order.
     * After getting a segment, process it, and then call ListContainers again (passing the the previously-returned
     * Marker) to get the next segment.
     * For more information, see https://docs.microsoft.com/rest/api/storageservices/list-containers2.
     * @param prefix
     *      A {@code String} that represents the prefix of the container name.
     * @param marker
     *      A {@code String} that identifies the portion of the list of containers to be returned with the next listing operation.
     * @param maxresults
     *      A {@code Integer} representing the maximum number of results to retrieve.  If {@code null} or greater
 *          than 5000, the server will return up to 5,000 items.  Must be at least 1.
     * @param include
     *      A {@code String} representing which details to include when listing the containers in this storage account.
     * @param timeout
     * @return
     */
    public Single<RestResponse<ServiceListContainersHeaders, ListContainersResponse>> listConatinersAsync(
            String prefix, String marker, Integer maxresults, ListContainersIncludeType include, Integer timeout) {
        return this.storageClient.services().listContainersWithRestResponseAsync(this.url, prefix, marker,
                maxresults, include, null, null);
    }

    /**
     * Creates a new {@link ServiceURL} with the given pipeline.
     * @param pipeline
     *      A {@link HttpPipeline} object to set.
     * @return
     *      A {@link ServiceURL} object with the given pipeline.
     */
    public ServiceURL withPipeline(HttpPipeline pipeline) {
        return new ServiceURL(super.url, pipeline);
    }
}
