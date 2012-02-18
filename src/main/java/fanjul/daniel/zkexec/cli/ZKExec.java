package fanjul.daniel.zkexec.cli;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;
import uk.co.flamingpenguin.jewel.cli.CliFactory;

import com.google.common.io.ByteStreams;

public class ZKExec {

    public static void main(final String[] args) throws ArgumentValidationException {

        final Logger root = Logger.getRootLogger();
        root.addAppender(new NullAppender());

        final ZKExec zkExec = new ZKExec(args);
        final int status = zkExec.run();

        System.exit(status);
    }

    public ZKExec(final String[] args) throws ArgumentValidationException {
        this(CliFactory.parseArguments(ZKExecOptions.class, args));
    }

    private final ZKExecOptions options;

    public ZKExec(final ZKExecOptions options) {
        this.options = options;
    }

    public ZKExecOptions getOptions() {
        return this.options;
    }

    private String getRootNodeName() {
        return this.options.getZNodeName();
    }

    private String getLockNodeName() {
        return this.getRootNodeName() + "/lock";
    }

    private String getProcNodeName() {
        return this.getRootNodeName() + "/proc";
    }

    private String getProcessNodeNamePrefix() {
        return this.getProcNodeName() + "/process-";
    }

    public int run() {

        ZooKeeper zooKeeper = null;
        try {

            final LinkedList<WatchedEvent> events = new LinkedList<WatchedEvent>();
            zooKeeper = new ZooKeeper(this.options.getConnectionString(), this.options.getSessionTimeout(), new Watcher() {

                @Override
                public void process(final WatchedEvent event) {
                    synchronized (events) {
                        events.add(event);
                        events.notifyAll();
                    }
                }
            });

            final List<ACL> acl = new LinkedList<ACL>();
            Long maxProcesses = this.getMaxProcessesAndAcl(zooKeeper, acl);
            if (maxProcesses == null) {
                this.close(zooKeeper);
                return this.runProcess();
            }

            List<String> children = null;
            while (true) {

                if (children == null || children.size() < maxProcesses) {
                    boolean locked;
                    try {
                        this.info("creating ephemeral znode " + this.getLockNodeName());
                        zooKeeper.create(this.getLockNodeName(), null, acl, CreateMode.EPHEMERAL);
                        locked = true;
                    } catch (final NodeExistsException e) {
                        this.warn(e);
                        locked = false;
                    }
                    String processNodeName = null;
                    if (locked) {
                        children = this.getChildren(zooKeeper, acl);
                        if (children.size() < maxProcesses) {
                            this.info("creating ephemeral sequential znode " + this.getProcessNodeNamePrefix());
                            processNodeName = zooKeeper.create(this.getProcessNodeNamePrefix(), null, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
                            this.info("created znode " + processNodeName);
                        }
                        this.info("deleting znode " + this.getLockNodeName());
                        zooKeeper.delete(this.getLockNodeName(), -1);
                    }
                    if (processNodeName != null) {
                        final int status = this.runProcess();
                        this.close(zooKeeper);
                        return status;
                    }
                }

                boolean nodeDataChanged = false;
                boolean nodeChildrenChanged = false;
                while (!nodeDataChanged && !nodeChildrenChanged) {
                    synchronized (events) {

                        if (events.isEmpty()) {
                            try {
                                this.info("waiting notifications");
                                events.wait();
                            } catch (final InterruptedException e) {
                                this.warn(e);
                            }
                        }

                        while (!events.isEmpty()) {
                            final WatchedEvent event = events.pop();
                            if (event.getType() == EventType.NodeDataChanged && this.getRootNodeName().equals(event.getPath())) {
                                nodeDataChanged = true;
                                this.info("detected data change on " + this.getRootNodeName());
                            } else if (event.getType() == EventType.NodeChildrenChanged && this.getProcNodeName().equals(event.getPath())) {
                                nodeChildrenChanged = true;
                                this.info("detected children change on " + this.getProcNodeName());
                            } else if (event.getType() == EventType.None && event.getPath() == null) {
                            } else {
                                this.warn("ignored event " + event);
                            }
                        }
                    }
                }

                if (nodeDataChanged) {
                    maxProcesses = this.getMaxProcessesAndAcl(zooKeeper, acl);
                    if (maxProcesses == null) {
                        this.close(zooKeeper);
                        return this.runProcess();
                    }
                }

                children = this.getChildren(zooKeeper, acl);
            }

        } catch (final ZKExecException e) {
            this.close(zooKeeper);
            throw e;
        } catch (final Exception e) {
            this.error(e);
            if (zooKeeper != null) {
                this.close(zooKeeper);
            }
            return this.runProcess();
        }
    }

    private Long getMaxProcessesAndAcl(final ZooKeeper zooKeeper, final List<ACL> acl) throws KeeperException, InterruptedException {

        final byte[] data;
        try {
            acl.clear();
            this.info("getting acl of " + this.getRootNodeName());
            acl.addAll(zooKeeper.getACL(this.getRootNodeName(), new Stat()));
            this.info("getting data of " + this.getRootNodeName());
            data = zooKeeper.getData(this.getRootNodeName(), true, null);
        } catch (final NoNodeException e) {
            this.error("znode " + this.getRootNodeName() + " does not exist");
            return null;
        }

        if (data == null) {
            this.error("znode " + this.getRootNodeName() + " has no data");
            return null;
        }

        String line;
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), Charset.forName("utf-8")));
            line = reader.readLine();
        } catch (final Exception e) {
            this.error(e);
            return null;
        }

        if (line == null) {
            this.error("znode " + this.getRootNodeName() + " has no data");
            return null;
        }

        try {
            final long maxProcesses = Long.parseLong(line);
            this.info("maxProcesses = " + maxProcesses);
            return maxProcesses;
        } catch (final Exception e) {
            this.error("znode " + this.getRootNodeName() + " has invalid data: " + e);
            return null;
        }
    }

    private List<String> getChildren(final ZooKeeper zooKeeper, final List<ACL> acl) throws KeeperException, InterruptedException {

        while (true) {
            try {
                this.info("getting children of " + this.getProcNodeName());
                List<String> children = zooKeeper.getChildren(this.getProcNodeName(), true);
                if (children == null) {
                    children = new LinkedList<String>();
                }
                this.info("children of " + this.getProcNodeName() + ": " + children);
                return children;
            } catch (final NoNodeException e) {
                this.warn(e);
                try {
                    this.info("creating persistent znode " + this.getProcNodeName());
                    zooKeeper.create(this.getProcNodeName(), null, acl, CreateMode.PERSISTENT);
                } catch (final NodeExistsException e2) {
                    this.warn(e);
                }
            }
        }
    }

    private int runProcess() {
        try {
            final List<String> commandLine = this.options.getCommandLine();
            if (this.options.getVerbose()) {
                this.info("running " + commandLine);
            }

            final Process process = Runtime.getRuntime().exec(commandLine.toArray(new String[commandLine.size()]));
            this.copy(System.in, process.getOutputStream());
            this.copy(process.getInputStream(), System.out);
            this.copy(process.getErrorStream(), System.err);
            process.waitFor();
            return process.exitValue();
        } catch (final Exception e) {
            throw new ZKExecException("running command line", e);
        }
    }

    private void copy(final InputStream in, final OutputStream out) {

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ByteStreams.copy(in, out);
                } catch (final IOException e) {
                    ZKExec.this.warn(e);
                }
            }
        }).start();
    }

    private void close(final ZooKeeper zooKeeper) {
        try {
            zooKeeper.close();
        } catch (final Exception e) {
            this.warn(e);
        }
    }

    private void info(final String message) {
        if (this.options.getVerbose()) {
            System.out.println("zkexec: INFO: " + message);
        }
    }

    private void warn(final Exception e) {
        this.warn(e.getMessage());
    }

    private void warn(final String message) {
        if (this.options.getVerbose()) {
            System.err.println("zkexec: WARN: " + message);
        }
    }

    private void error(final Exception e) {
        this.error(e.getMessage());
    }

    private void error(final String message) {
        if (!this.options.getQuiet()) {
            System.err.println("zkexec: ERROR: " + message);
        }
    }
}
