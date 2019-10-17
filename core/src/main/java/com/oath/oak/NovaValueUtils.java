package com.oath.oak;

import java.nio.ByteBuffer;

public interface NovaValueUtils {
    enum NovaResult {
        TRUE, FALSE, RETRY
    }

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getValueByteBufferNoHeaderPrivate(Slice s);

    ByteBuffer getValueByteBufferNoHeader(Slice s);

    NovaResult lockRead(Slice s, int version);

    NovaResult unlockRead(Slice s, int version);

    NovaResult lockWrite(Slice s, int version);

    NovaResult unlockWrite(Slice s);

    NovaResult deleteValue(Slice s, int version);

    NovaResult isValueDeleted(Slice s, int version);

    int getOffHeapVersion(Slice s);
}
