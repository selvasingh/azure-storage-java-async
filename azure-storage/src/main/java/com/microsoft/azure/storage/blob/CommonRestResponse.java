package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.models.BlobPutHeaders;
import com.microsoft.azure.storage.models.BlockBlobPutBlockListHeaders;
import com.microsoft.rest.v2.RestResponse;

public final class CommonRestResponse {

    private RestResponse<BlobPutHeaders, Void> putBlobResponse;

    private RestResponse<BlockBlobPutBlockListHeaders, Void> putBlockListResponse;

    private CommonRestResponse(RestResponse<BlobPutHeaders, Void> response) {
        this.putBlobResponse = response;
    }

    private CommonRestResponse(RestResponse<BlockBlobPutBlockListHeaders, Void> response) {
        this.putBlockListResponse = response;
    }
}
