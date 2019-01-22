package com.oath.oak.NativeAllocator;


import com.oath.oak.OakMap;
import com.oath.oak.OakMapBuilder;
import com.oath.oak.StringComparator;
import com.oath.oak.StringSerializer;
import com.sun.management.HotSpotDiagnosticMXBean;
import org.junit.Test;

import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;

public class MemoryReleaseTest {



    @Test(timeout = 300_000)
    public void testOakRelease() {

        String val = String.format("-%016000d", 0);

        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(1024)
                .setChunkBytesPerItem(4096)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("");
        OakMap<String, String> oak =  builder.build();

        int i = 0;
        try {
            while (true) {
                String key = String.format("-%01024d", i++);
                oak.put(key, val);
            }
        } catch (OutOfMemoryError e) {

        }
        oak.close();
        long maxDirectMemorySize = Long.valueOf(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                .getVMOption("MaxDirectMemorySize").getValue());
        long blocks = maxDirectMemorySize/BlocksPool.getInstance().blockSize();
        assertEquals(blocks, BlocksPool.getInstance().numOfRemainingBlocks());
    }
}
