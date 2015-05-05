package com.leansoft.bigqueue.perf;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

public class BigQueuePeTest {

    private static final int COUNT = 100 * 1000;

    IBigQueue bigQueue;
    BlockingQueue<String> blockingQueue = new LinkedBlockingQueue<String>(COUNT);

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getenv("TEMP");
        if (!tempDir.endsWith(File.separator)) {
            tempDir += File.separator;
        }
        String dnQueueDir = tempDir + "dnQueue";
        File f = new File(dnQueueDir);
        delete(f);
        System.out.println("delete dir: " + f.getPath());

        try {
            bigQueue = new BigQueueImpl(tempDir, "dnQueue");
        } catch (IOException e) {
            fail("fail to init big queue");
        }
    }

    void delete(File f) {
        if (f != null && f.exists()) {
            if (f.isFile()) {
                f.delete();
            } else {
                File[] files = f.listFiles();
                for (File ff : files) {
                    delete(ff);
                }
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        if (bigQueue != null) {
            bigQueue.removeAll();
        }
    }

    /**
     * <pre>
     * size: bigqueue-0.7.1-SNAPSHOT.jar 40,302 bytes
     * 
     * insert 100000 DNs...
     * used time: 0.35s insert speed: 285714.29 DN/s
     * 
     * poll 100000 DNs...
     * used time: 0.20s insert speed: 497512.44 DN/s
     * 
     * JDK in-memory queue
     * 
     * insert 100000 DNs...
     * used time: 0.06s insert speed: 1818181.82 DN/s
     * 
     * poll 100000 DNs...
     * used time: 0.02s insert speed: 5555555.56 DN/s
     * </pre>
     */
    @Test
    public void test() {

        insert();
        poll();

        System.out.println("\nJDK in-memory queue");
        insertBq();
        pollBq();
    }

    private void insert() {
        System.out.println();
        System.out.println("insert " + COUNT + " DNs...");
        long from = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
            String dn = "PLMN-PLMN/MRBTS-" + i;
            try {
                bigQueue.enqueue(dn.getBytes());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        long to = System.currentTimeMillis();

        double dur = (to - from) / 1000.0;
        double speed = COUNT / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
        assertEquals(COUNT, bigQueue.size());
    }

    private void poll() {
        System.out.println();
        System.out.println("poll " + COUNT + " DNs...");
        long from = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
            try {
                String dn = new String(bigQueue.dequeue());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        long to = System.currentTimeMillis();

        double dur = (to - from) / 1000.0;
        double speed = COUNT / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
        assertEquals(0, bigQueue.size());
    }

    private void insertBq() {
        System.out.println();
        System.out.println("insert " + COUNT + " DNs...");

        long from = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
            String dn = "PLMN-PLMN/MRBTS-" + i;
            blockingQueue.add(dn);
        }
        long to = System.currentTimeMillis();

        double dur = (to - from) / 1000.0;
        double speed = COUNT / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
        assertEquals(COUNT, blockingQueue.size());
    }

    private void pollBq() {
        System.out.println();
        System.out.println("poll " + COUNT + " DNs...");

        long from = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
            String dn = blockingQueue.poll();
        }
        long to = System.currentTimeMillis();

        double dur = (to - from) / 1000.0;
        double speed = COUNT / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
        assertEquals(0, blockingQueue.size());
    }
}
