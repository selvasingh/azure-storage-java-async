package com.microsoft.azure.storage.blob;

public class BlobRange {

    private static BlobRange defaultBlobRange;

    public Long offset;

    public Long count;

    public BlobRange(Long offset, Long count) {
        if (offset != null && offset < 0) {
            throw new IllegalArgumentException("BlobRange offset must be greater than or equal to 0 if specified.");
        }
        if (offset != null && count < 0) {
            throw new IllegalArgumentException("BlobRange count must be greater than or equal to 0 if specified.");
        }
        this.offset = offset;
        this.count = count;
    }

    @Override
    public String toString() {
        if (offset != null) {
            long rangeStart = offset;
            long rangeEnd;
            if (count != null) {
                rangeEnd = offset + count - 1;
                return String.format(
                        Utility.LOCALE_US, Constants.HeaderConstants.RANGE_HEADER_FORMAT, rangeStart, rangeEnd);
            }

            return String.format(
                    Utility.LOCALE_US, Constants.HeaderConstants.BEGIN_RANGE_HEADER_FORMAT, rangeStart);
        }

        return null;
    }

    public static BlobRange getDefault() {
        if(defaultBlobRange == null) {
            defaultBlobRange = new BlobRange(null, null);
        }
        return defaultBlobRange;
    }
}
