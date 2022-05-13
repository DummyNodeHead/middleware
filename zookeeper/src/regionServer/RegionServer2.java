package regionServer;

import java.io.IOException;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/*
* 这个文件是RegionServer的复制
* 用来测试Master是否能监听多个rs并获取其中信息
* 测试结果为可以监听，RS也能正常运行
*
* */
public class RegionServer2 {
    final private static String ip = "127.0.0.1:2181"; // zookeeper集群地址
    final private static int timeout = 2000; // 超时时间
    private ZooKeeper zk = null; // 建立的连接
    private String rsIp;
    private String rsPort;
    private String rsName;

    public void register() throws InterruptedException, KeeperException {
        // 插入RegionServer节点

        zk.create("/rslist/" + rsName, (rsIp + ":" + rsPort).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println("rs " + rsName + " online");
    }

    public void connect() throws IOException {
        zk = new ZooKeeper(ip, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent we) {

            }
        });
    }

    public void run() throws InterruptedException {
        while (true)
            ;
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        RegionServer2 regionServer = new RegionServer2();
        Scanner in = new Scanner(System.in);

        System.out.println("input your rs ip and port");
        System.out.print("ip:");
        regionServer.rsIp = in.next();
        System.out.print("port:");
        regionServer.rsPort = in.next();
        System.out.println("input your rs name");
        regionServer.rsName = in.next();

        regionServer.connect();
        regionServer.register();
        regionServer.run();
    }
}
