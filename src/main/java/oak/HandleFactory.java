/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

class HandleFactory {

    private boolean offHeap; // for future use, currently should always be true

    HandleFactory(boolean offHeap) {
        this.offHeap = offHeap;
    }

    Handle createHandle() {
        return new HandleOffHeapImpl(null, 0);
    }

}
