package com.microsoft.azure.storage.blob;

public class BlobRange {

    public Long offset;

    public Long count;

    public BlobRange(Long offset, Long count) {
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
}
