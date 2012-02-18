package fanjul.daniel.zkexec;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fanjul.daniel.zkexec.cli.ZKExec;

public class ZooKeeperTest {

    private static final String PORT = "4567";

    class TestZooKeeperServerMain extends ZooKeeperServerMain {

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    private TestZooKeeperServerMain server;
    private ZooKeeper client;
    private Thread thread;

    @Before
    public void setup() throws Exception {

        final Logger root = Logger.getRootLogger();
        root.addAppender(new NullAppender());

        this.server = new TestZooKeeperServerMain();
        this.thread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    final ServerConfig config = new ServerConfig();
                    config.parse(new String[] { PORT, "target/test-" + UUID.randomUUID(), });
                    ZooKeeperTest.this.server.runFromConfig(config);
                } catch (final Exception e) {
                }
            }
        });
        this.thread.setName("zookeeper-server");
        this.thread.start();
        Thread.sleep(250);
        this.client = new ZooKeeper("localhost:" + PORT, 5000, null);
    }

    @After
    public void teardown() {
        try {
            this.server.shutdown();
        } catch (final Exception e) {
        }
        try {
            this.client.close();
        } catch (final Exception e) {
        }
        try {
            this.thread.join();
        } catch (final Exception e) {
        }
    }

    @Test
    public void testX() throws Exception {

        final String nodeName = "/test";

        final byte[] maxProcesses = "1".getBytes(Charset.forName("utf-8"));
        this.client.create(nodeName, maxProcesses, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertEquals(0, new ZKExec(new String[] { "-z", "localhost:" + PORT, "-n", nodeName, "-q", "true" }).run());
        assertEquals(1, new ZKExec(new String[] { "-z", "localhost:" + PORT, "-n", nodeName, "-q", "false" }).run());
    }
}
