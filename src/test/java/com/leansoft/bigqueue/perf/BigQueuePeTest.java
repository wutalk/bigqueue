package com.leansoft.bigqueue.perf;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

public class BigQueuePeTest {

    IBigQueue bigQueue;

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

    int count = 100 * 1000;

    /**
     * <pre>
     * insert 100000 DNs...
     * used time: 0.36s insert speed: 277777.78 DN/s
     * 
     * poll 100000 DNs...
     * used time: 0.22s insert speed: 454545.45 DN/s
     * </pre>
     */
    @Test
    public void test() {

        insert();
        poll();
    }

    private void insert() {
        System.out.println();
        System.out.println("insert " + count + " DNs...");
        long from = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
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
        double speed = count / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
    }

    private void poll() {
        System.out.println();
        System.out.println("poll " + count + " DNs...");
        long from = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            try {
                String dn = new String(bigQueue.dequeue());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        long to = System.currentTimeMillis();

        double dur = (to - from) / 1000.0;
        double speed = count / dur;
        System.out.format("used time: %.2fs insert speed: %.2f DN/s\r\n", dur, speed);
    }
}
