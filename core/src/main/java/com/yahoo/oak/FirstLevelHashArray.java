package com.yahoo.oak;

// the array of pointers to HashChunks according to keyHash most significant bits
class FirstLevelHashArray<K,V> {

  private final HashChunk<K, V>[] chunks;    // array is initialized to 0 - this is important!
  private final int msbForFirstLevelHash;


  FirstLevelHashArray(int msbForFirstLevelHash) {
    this.chunks = new HashChunk[msbForFirstLevelHash];
    this.msbForFirstLevelHash = msbForFirstLevelHash;

    // least significant bits remaining
    int lsbForSecondLEvel = Integer.SIZE - msbForFirstLevelHash;

    // size of HashChunk should be 2^lsbForSecondLEvel
    int chunkSize = (int) (Math.pow(2, lsbForSecondLEvel) + 1);

  }
}
