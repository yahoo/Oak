package oak;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class KeysManager {

    abstract int length();

    abstract void writeKey(ByteBuffer key, int ki, int length);

    abstract void writeKey(Object key,
                           Consumer<Entry<Entry<ByteBuffer, Integer>, Object>> keyCreator,
                           int ki);

    abstract ByteBuffer getKeys();

    abstract void release();

    abstract void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy);

    abstract int getPosition();

}
