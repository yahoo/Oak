package oak;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class IntComparator implements Comparator<ByteBuffer> {

    @Override
    public int compare(ByteBuffer bb1, ByteBuffer bb2) {
        int i1 = bb1.getInt(bb1.position());
        int i2 = bb2.getInt(bb2.position());
        if (i1 > i2) {
            return 1;
        } else if (i1 < i2) {
            return -1;
        } else {
            return 0;
        }
    }

}
