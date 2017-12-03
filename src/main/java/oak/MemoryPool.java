package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;

interface MemoryPool {

    Pair<Integer, ByteBuffer> allocate(int capacity);

    void free(int i, ByteBuffer bb);

    long allocated();

    void clean();

}
