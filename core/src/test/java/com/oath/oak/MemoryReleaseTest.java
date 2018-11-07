package com.oath.oak;


import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import org.junit.Test;

import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        long a = Long.valueOf(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                .getVMOption("MaxDirectMemorySize").getValue());
        long blocks = a/BlocksPool.BLOCK_SIZE;
        assertEquals(blocks, BlocksPool.getInstance().numOfRemainingBlocks());
    }
}
