package com.oath.oak;

import java.util.function.Function;

public interface OakTransformer<T> extends Function<OakReadBuffer, T> {
}
