package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;

class ValueFactory {

    boolean offHeap;

    ValueFactory(boolean offHeap) {
        this.offHeap = offHeap;
    }

    Pair<Integer, ByteBuffer> createValue(ByteBuffer value, OakMemoryManager memoryManager) {
        int rem = value.remaining();
        int pos = value.position();
        ByteBuffer newVal;
        int i = 0;
        if (!offHeap) {
            assert memoryManager == null;
            byte[] valueArray = new byte[rem]; // TODO remove this array
            System.arraycopy(value.array(), value.position(), valueArray, 0, value.remaining());
            newVal = ByteBuffer.wrap(valueArray);
        } else {
            Pair<Integer, ByteBuffer> pair = memoryManager.allocate(rem);
            i = pair.getKey();
            newVal = pair.getValue();
            for (int j = 0; j < rem; j++) {
                newVal.put(j, value.get(pos + j));
            }
        }
        return new Pair<>(i, newVal);
    }

}
