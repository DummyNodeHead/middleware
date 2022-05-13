package master;

import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class MasterInitializer {
    
    protected int Access_threshold=5;//访问阈值，超出该值后进行负载均衡
    

    protected CuratorFramework client; // 这里的 client 指的就是 Master Server

    /*
     * <String region_server_addr, Integer cnt>
     * addr 指的是 ip:port
     * 统计 region server 被访问的次数
     */
    protected Map<String, Integer> regionFreq;

    /**
     * <String table_name, Integer cnt>
     */
    protected Map<String, Integer> tableFreq;

    /**
     * <String region_server_addr, Socket socket>
     * master 和对应 region 建立的 socket 连接
     */
    protected Map<String, Socket> socketList;

    /*
     * <String region_server_addr, List<String> table_name>
     * addr 指的是 ip:port
     * 记录一个 region 所保存的所有 table
     */
    protected Map<String, List<String>> regionTable;

    /*
     * <String table_name0, String addr>
     * master table 所在 region 的 addr
     */
    protected Map<String, String> mTableAddr;

    /*
     * <String table_name1, String addr>
     * slave table 所在 region 的 addr
     */
    protected Map<String, String> sTableAddr;

    final protected static String ip = "127.0.0.1:2181"; // zookeeper集群地址

    final protected String masterPath = "/master"; // master节点的path

    public MasterInitializer() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10); // 重试策略

        client = CuratorFrameworkFactory.builder().connectString(ip).sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000).retryPolicy(retryPolicy).build();
        regionFreq = new HashMap<>();
        tableFreq = new HashMap<>();
        regionTable = new HashMap<>();
        mTableAddr = new HashMap<>();
        sTableAddr = new HashMap<>();
        socketList = new HashMap<>();
    }

    public boolean isNodeExist(String path) {
        try {
            if (client.checkExists().forPath(path) != null) {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

}
