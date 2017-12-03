package oak;

import javafx.util.Pair;

import java.nio.ByteBuffer;

public class KeysManagerOffHeapImpl extends KeysManager {

    ByteBuffer keys;
    int i;
    OakMemoryManager memoryManager;

    KeysManagerOffHeapImpl(int bytes, OakMemoryManager memoryManager) {
        Pair<Integer, ByteBuffer> pair = memoryManager.allocate(bytes);
        i = pair.getKey();
        keys = pair.getValue();
        this.memoryManager = memoryManager;
    }

    @Override
    int length() {
        return keys.remaining();
    }

    @Override
    void writeKey(ByteBuffer key, int ki, int length) {
        int keyPos = key.position();
        int myPos = keys.position();
        for (int j = 0; j < length; j++) {
            keys.put(myPos + ki + j, key.get(keyPos + j));
        }
    }

    @Override
    ByteBuffer getKeys() {
        return keys;
    }

    @Override
    void release() {
        memoryManager.release(i, keys);
    }

    @Override
    void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
        ByteBuffer srcKeys = srcKeysManager.getKeys();
        int srcKeyPos = srcKeys.position();
        int myPos = keys.position();
        for (int j = 0; j < lengthToCopy; j++) {
            keys.put(myPos + index + j, srcKeys.get(srcKeyPos + srcIndex + j));
        }
    }

    @Override
    int getPosition() {
        return keys.position();
    }
}
