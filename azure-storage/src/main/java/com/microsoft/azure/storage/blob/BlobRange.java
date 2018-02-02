package com.microsoft.azure.storage.blob;

public final class BlobRange {

    public static final BlobRange DEFAULT = new BlobRange(0, 0);

    public final long offset;

    public final long count;

    public BlobRange(long offset, long count) {
        if (offset < 0) {
            throw new IllegalArgumentException("BlobRange offset must be greater than or equal to 0 if specified.");
        }
        if (count < 0) {
            throw new IllegalArgumentException("BlobRange count must be greater than or equal to 0 if specified.");
        }
        this.offset = offset;
        this.count = count;
    }

    @Override
    public String toString() {

        if (count != 0) {
            long rangeEnd = this.offset + this.count - 1;
            return String.format(
                    Utility.LOCALE_US, Constants.HeaderConstants.RANGE_HEADER_FORMAT, this.offset, rangeEnd);
        }

        return String.format(
                Utility.LOCALE_US, Constants.HeaderConstants.BEGIN_RANGE_HEADER_FORMAT, this.offset);
    }
}
