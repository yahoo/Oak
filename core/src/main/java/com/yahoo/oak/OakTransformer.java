/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Function;

public interface OakTransformer<T> extends Function<OakScopedReadBuffer, T> {
}
