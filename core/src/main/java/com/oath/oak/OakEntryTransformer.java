package com.oath.oak;


import java.util.Map;
import java.util.function.Function;

public interface OakEntryTransformer<T> extends Function<Map.Entry<OakReadBuffer, OakReadBuffer>, T> {
}
