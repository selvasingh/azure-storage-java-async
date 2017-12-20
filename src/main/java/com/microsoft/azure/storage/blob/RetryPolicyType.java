package com.microsoft.azure.storage.blob;

public enum RetryPolicyType {
    // tells the pipeline to use an exponential back-off retry policy
	EXPONENTIAL,

    // tells the pipeline to use a fixed back-off retry policy
    FIXED
}
