package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface OakTransformer<T> extends Function<ByteBuffer, T> {
}
