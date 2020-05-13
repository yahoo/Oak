package com.oath.oak;

public class KeyBuffer extends OakAttachedReadBuffer {
    public KeyBuffer() {
        super(0);
    }

    public KeyBuffer(KeyBuffer key) {
        super(key);
    }
}
