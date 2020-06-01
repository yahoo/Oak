package com.oath.oak;

public class KeyBuffer extends ScopedReadBuffer {
    public KeyBuffer() {
        super(0);
    }

    public KeyBuffer(KeyBuffer key) {
        super(key);
    }
}
