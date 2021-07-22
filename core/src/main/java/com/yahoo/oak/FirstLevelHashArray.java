package com.yahoo.oak;

// the array of pointers to HashChunks according to keyHash most significant bits
class FirstLevelHashArray<K,V> {

  private final HashChunk<K, V>[] chunks;    // array is initialized to 0 - this is important!

  FirstLevelHashArray(int msbForFirstLevelHash) {
    this.chunks = new HashChunk[msbForFirstLevelHash];
  }
}
