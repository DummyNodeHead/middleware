package client;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Client {
    final private static String zkIp = "127.0.0.1:2181"; // zookeeper集群地址
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10); // 重试策略
    private CuratorFramework connect = CuratorFrameworkFactory.builder().connectString(zkIp).sessionTimeoutMs(60 * 1000)
            .connectionTimeoutMs(15 * 1000).retryPolicy(retryPolicy).build();

    private class ServerListener implements Runnable {
        @Override
        public void run() {
            // 监听本服务器对应的节点
            CuratorCache curatorCache = CuratorCache.builder(connect, "/server").build();
            CuratorCacheListener listener = CuratorCacheListener.builder().forChanges((oldNode, node) -> {
                        String oldData = new String(oldNode.getData());
                        String newData = new String(node.getData());
                    }).forCreates(node -> {
                        System.out.println("A new server is online");
                    }).forDeletes(node -> {
                        System.out.println("A server is offline");
                    })
                    .build();
            curatorCache.listenable().addListener(listener);
            curatorCache.start();
        }
    }

    public void connectZK() {

    }
}