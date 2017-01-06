package io.indexr.server;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.directory.api.util.Strings;
import org.apache.zookeeper.CreateMode;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.indexr.segment.pack.PackDurationStat;
import io.indexr.server.rt.RealtimeConfig;
import io.indexr.util.GlobalExecSrv;
import io.indexr.util.Try;

public class IndexRNode implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(IndexRNode.class);

    private TablePool tablePool;
    private IndexRConfig config;
    private Server httpServer;
    private Future printStat;
    private Future declareNode;

    public IndexRNode(String host) throws Exception {
        RealtimeConfig.loadSubtypes();
        this.config = new IndexRConfig();
        CuratorFramework zkClient = config.getZkClient();
        this.tablePool = new TablePool(
                host,
                zkClient,
                config.getFileSystem(),
                config);
        httpService();

        String path = IndexRConfig.zkHostDeclarePath(host);
        ZkHelper.createIfNotExist(zkClient, path, CreateMode.EPHEMERAL);

        printStat = GlobalExecSrv.EXECUTOR_SERVICE.scheduleAtFixedRate(
                PackDurationStat::print,
                1, 1, TimeUnit.MINUTES);
        // Redeclare node to zk after a period of time.
        declareNode = GlobalExecSrv.EXECUTOR_SERVICE.scheduleAtFixedRate(
                () -> Try.on(() -> ZkHelper.createIfNotExist(zkClient, path, CreateMode.EPHEMERAL), 1, logger, "Failed to declare indexr node in ZK"),
                0, 10, TimeUnit.MINUTES);
    }

    public IndexRNode() throws Exception {
        this(InetAddress.getLocalHost().getHostName());
    }

    public TablePool getTablePool() {
        return tablePool;
    }

    public TablePool getTableManager() {
        return tablePool;
    }

    public IndexRConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {
        if (tablePool != null) {
            IOUtils.closeQuietly(tablePool);
            tablePool = null;
        }
        if (config != null) {
            IOUtils.closeQuietly(config);
            config = null;
        }
        if (httpServer != null) {
            Try.on(httpServer::stop, logger);
            httpServer = null;
        }
        if (printStat != null) {
            printStat.cancel(true);
            printStat = null;
        }
        if (declareNode != null) {
            declareNode.cancel(true);
            declareNode = null;
        }
    }

    public boolean stopRealtime() {
        tablePool.stopRealtime();
        return true;
    }

    public boolean startRealtime() {
        tablePool.startRealtime();
        return true;
    }

    public boolean stopNode() {
        tablePool.stopRealtime();
        while (!tablePool.isSafeToExit()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    private void httpService() throws Exception {
        int port = config.getControlPort();
        httpServer = new Server(port);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        httpServer.setHandler(context);

        context.addServlet(new ServletHolder(new NodeServlet()), "/control");

        httpServer.start();
        logger.info("Start control service at: {}", port);
    }

    private class NodeServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String cmd = req.getParameter("cmd");
            if (Strings.isEmpty(cmd)) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().println("Please specify cmd");
                return;
            }
            boolean ok = true;
            boolean exit = false;
            try {
                switch (cmd.toLowerCase()) {
                    case "stoprt":
                        ok = stopRealtime();
                        break;
                    case "startrt":
                        ok = startRealtime();
                        break;
                    case "stopnode":
                        ok = stopNode();
                        exit = true;
                        break;
                    default:
                        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                        resp.getWriter().printf("Not support cmd [%s].", cmd);
                        return;
                }
            } catch (Exception e) {
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                e.printStackTrace(resp.getWriter());
            }
            if (ok) {
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println("OK");
                if (exit) {
                    resp.getWriter().flush();
                    resp.getWriter().close();
                    logger.info("IndexR node exit.");
                    System.exit(0);
                }
            } else {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().println("FAIL");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        IndexRNode node = new IndexRNode("localhost");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Try.on(node::close, logger);
            }
        });
        System.in.read();
    }
}
