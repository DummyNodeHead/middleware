package server;

import master.MasterServer;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server1 {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10); // 重试策略
    private CuratorFramework connect = CuratorFrameworkFactory.builder().connectString(ip).sessionTimeoutMs(60 * 1000)
            .connectionTimeoutMs(15 * 1000).retryPolicy(retryPolicy).build();
    final private static String ip = "127.0.0.1:2181"; // zookeeper集群地址
    final private String serverPath = "/server"; // master节点的path
    final private String serverName = "server1";
    final private int serverPort = 3333;

    private class HandleClient implements Runnable {
        ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();

        @Override
        public void run() {
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(serverPort);
                while (true) {

                    // 监听，等待客户端连接
                    Socket socket = serverSocket.accept();
                    System.out.println(
                            "Accepted a client: " + socket.getInetAddress().toString() + ":" + socket.getPort()
                                    + " local port: " + socket.getLocalPort());

                    // 从线程池创建一个线程
                    newCachedThreadPool.execute(() -> handler(socket));
                }
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }

        private void handler(Socket client) {
            System.out.println("one client connected...");

            // 业务逻辑可写在此

        }
    }

    private class HandleConfig implements Runnable {
        @Override
        public void run() {
            // 监听本服务器对应的节点
            CuratorCache curatorCache = CuratorCache.builder(connect, serverPath+"/"+serverName).build();
            CuratorCacheListener listener = CuratorCacheListener.builder().forChanges((oldNode, node) -> {
                String oldData=new String(oldNode.getData());
                String newData=new String(node.getData());
                System.out.println(serverName+"'s config changed");
            }).forInitialized(() -> System.out.println(serverName+" is online")).build();
            curatorCache.listenable().addListener(listener);
            curatorCache.start();
        }
    }


    public void serverOnline() {
        connect.start();

        try {
            /*
             * 在zk集群中插入表示master的节点
             * 查找master节点，看是否已经存在master
             * 如果不存在，则创建master节点
             */
            if (isNodeExist(serverPath + '/' + serverName)) {
                throw new RuntimeException("Server already online!");
            }
            // 创建临时节点
            connect.create().withMode(CreateMode.EPHEMERAL).forPath(serverPath + '/' + serverName, ("127.0.0.1:" + serverPort).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        Thread clientThread = new Thread(new HandleClient());
        clientThread.start();

        Thread configThread = new Thread(new HandleConfig());
        configThread.start();
    }

    public boolean isNodeExist(String path) {
        try {
            if (connect.checkExists().forPath(path) != null) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
