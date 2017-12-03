package oak;

import java.nio.ByteBuffer;

public abstract class KeysManager {

    abstract int length();

    abstract void writeKey(ByteBuffer key, int ki, int length);

    abstract ByteBuffer getKeys();

    abstract void release();

    abstract void copyKeys(KeysManager srcKeysManager, int srcIndex, int index, int lengthToCopy);

    abstract int getPosition();

}
