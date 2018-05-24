package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;

public class KeysManagerOnHeapImpl extends KeysManager {

    private final byte[] keys;

    KeysManagerOnHeapImpl(int bytes) {
        this.keys = new byte[bytes];
    }

    @Override
    int length() {
        return keys.length;
    }

    @Override
    void writeKey(ByteBuffer key, int ki, int length) {
        // TODO here we assume array
        System.arraycopy(key.array(), key.position(), keys, ki, length);
    }

    @Override
    void writeKey(Object key,
                  Consumer<Entry<Entry<ByteBuffer, Integer>, Object>> keyCreator,
                  int ki) {
        throw new UnsupportedOperationException();
    }

    @Override
    ByteBuffer getKeys() {
        return ByteBuffer.wrap(keys);
    }

    @Override
    void release() {
        // do nothing
    }

    @Override
    void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy) {
        // TODO here we assume array
        System.arraycopy(((KeysManagerOnHeapImpl) srcKeysManager).keys, srcIndex, keys, index, lengthToCopy);
    }

    @Override
    int getPosition() {
        return 0;
    }
}
