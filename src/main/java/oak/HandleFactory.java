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
