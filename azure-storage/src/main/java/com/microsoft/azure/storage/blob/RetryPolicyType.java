package com.microsoft.azure.storage.blob;

public enum RetryPolicyType {
    // Tells the pipeline to use an exponential back-off retry policy.
	EXPONENTIAL,

    // Tells the pipeline to use a fixed back-off retry policy.
    FIXED
}
