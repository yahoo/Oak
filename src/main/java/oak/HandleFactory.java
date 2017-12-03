package oak;

class HandleFactory {

    private boolean offHeap;

    HandleFactory(boolean offHeap) {
        this.offHeap = offHeap;
    }

    Handle createHandle() {
        if (offHeap) return new HandleOffHeapImpl(null, 0);
        return new HandleOnHeapImpl(null);
    }

}
