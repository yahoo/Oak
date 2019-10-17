package com.oath.oak;

import java.nio.ByteBuffer;

public interface NovaValueUtils {
    enum Result {
        TRUE, FALSE, RETRY
    }

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getValueByteBufferNoHeaderPrivate(Slice s);

    ByteBuffer getValueByteBufferNoHeader(Slice s);

    Result lockRead(Slice s, int version);

    Result unlockRead(Slice s, int version);

    Result lockWrite(Slice s, int version);

    Result unlockWrite(Slice s);

    Result deleteValue(Slice s, int version);

    Result isValueDeleted(Slice s, int version);

    int getOffHeapVersion(Slice s);
}
