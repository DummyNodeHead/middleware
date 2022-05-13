package regionServer;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import miniSQL.API;
import miniSQL.CATALOGMANAGER.CatalogManager;


/* 负责管理 region server 公共资源（包括 master 通信 socket、zookeeper 等 */
public class RegionServer {
    final private static String ip = "192.168.43.191:2181"; // zookeeper集群地址
    final private static int timeout = 2000; // 超时时间
    private ZooKeeper zk = null; // 建立的连接
    
    // master 相关信息和资源
    private String masterIp;
    private String masterPort;
    public Socket masterSocket;
    private DataInputStream masterReader;
    private DataOutputStream masterWriter;

    // 本机相关信息和资源
    private String rsIp;
    private String externalPort;  // 对 client 提供服务的端口
    private String internalPort;  // 对 master 和其他 rs 提供服务的端口
    private String rsName;

    private ExternalServer externalServer;  // 对 client 提供服务的接口和资源管理
    private InternalServer internalServer;  // 对其他 server 提供服务的接口和资源管理

    private Lock lock;  // 互斥锁，用于控制 miniSQL 的并发访问

    private HashMap<String, Integer> masterTbls;  // 记录本机上的主表及访问次数
    private RefCounterThread referenceCounter;    // 定期将本机主表访问次数同步给 master 的线程

    public RegionServer(String rsIp, String externalPort, String internalPort, String rsName) throws IOException {
        this.rsIp = rsIp;
        this.externalPort = externalPort;
        this.internalPort = internalPort;
        this.rsName = rsName;

        this.internalServer = new InternalServer(rsIp, internalPort, this);
        this.externalServer = new ExternalServer(rsIp, externalPort, this);

        /* 上线前清空本机所有 sql 文件 */
        File wd = new File(System.getProperty("user.dir"));
        System.out.println("Current working directory is " + wd + ".");
        for (File file: wd.listFiles()) {
            if (file.isDirectory()) {
                continue;
            } else {
                String fileName = file.getName();
                // System.out.println("Processing " + fileName + "...");
                if (fileName.equals("table_catalog") || fileName.equals("index_catalog") ||
                    fileName.endsWith(".table") || fileName.endsWith(".index")) {
                    file.delete();
                    System.out.println("Deleted " + fileName + ".");
                }
            }
        }

        try {
            API.initial();
        } catch (Exception e) {
            System.out.println("Failed to initialize miniSQL.");
        }
        // 初始时默认本机上的表都是主表
        masterTbls = new HashMap<>();
        for (String tableName: CatalogManager.getTableNames()) {
            masterTbls.put(tableName, 0);
        }
        referenceCounter = new RefCounterThread(masterTbls, this);

        lock = new ReentrantLock();
    }

    /***********************************************************************************
     *                               ZooKeeper 操作
     ***********************************************************************************/

    public void register() throws InterruptedException, KeeperException {
        // 设置 znode 内容（未考虑表名重复的情况）
        StringBuffer znodeData = new StringBuffer(rsIp + ":" + externalPort + ":" + rsIp + ":" + internalPort);
        for (String tableName: masterTbls.keySet()) {
            znodeData.append(":" + tableName);
        }

        // 获取 master 地址
        Stat stat = new Stat();
        String[] masterAddr = new String(zk.getData("/master", true, stat)).split(":");
        masterIp = masterAddr[0];
        masterPort = masterAddr[1];
        
        // 插入 RegionServer 节点
        zk.create("/rslist/" + rsName, znodeData.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("rs " + rsName + " online");
    }

    public void connect() throws IOException {
        zk = new ZooKeeper(ip, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent we) {
                ;
            }
        });
    }

    public void run() throws InterruptedException, IOException {
        Thread internalServerThread = new Thread(internalServer);
        internalServerThread.start();
        Thread externalServerThread = new Thread(externalServer);
        externalServerThread.start();
        //referenceCounter.start();
    }

    /***********************************************************************************
     *                                miniSQL 操作
     ***********************************************************************************/

    public void createMasterTable(String tableName) {
        masterTbls.put(tableName, 0);
        updateZnode();
    }

    public void dropMasterTable(String tableName) {
        masterTbls.remove(tableName);
        updateZnode();
    }

    /***********************************************************************************
     *                                 master 通信
     ***********************************************************************************/

    /* 向 master 发送一条整数消息 */
    public void sendIntToMaster(int i) throws IOException {
        System.out.println("Sent to master: " + i);
        masterWriter.writeInt(i);
    }
    
    /* 向 master 发送一条字符串消息 */
    public void sendUTFToMaster(String s) throws IOException {
        System.out.println("Sent to master: " + s);
        masterWriter.writeUTF(s);
    }

    /* 从 master 读取一条整数消息 */
    public int readIntFromMaster() throws IOException {
        return masterReader.readInt();
    }

    /* 从 master 读取一条字符串消息 */
    public String readUTFFromMaster() throws IOException {
        return masterReader.readUTF();
    }

    public String getMasterIp() {
        return masterIp;
    }

    public String getMasterPort() {
        return masterPort;
    }

    /* 设置 master 的通信 socket 及相应的输入输出流 */
    public void setMasterSocket(Socket socket) throws IOException {
        if (this.masterSocket != null) {
            this.masterSocket.close();
            System.out.println("Master socket closed.");
        }
        this.masterSocket = socket;
        if (socket != null) {
            System.out.println("Master socket set.");
            this.masterReader = new DataInputStream(socket.getInputStream());
            this.masterWriter = new DataOutputStream(socket.getOutputStream());
        }
    }

    /***********************************************************************************
     *                                 负载均衡
     ***********************************************************************************/

    /* 增加主表的引用次数 */
    public void addReference(String tableName) {
        if (masterTbls.get(tableName) == null) {
            masterTbls.put(tableName, 1);
        } else {
            masterTbls.put(tableName, masterTbls.get(tableName) + 1);
        }
    }

    /***********************************************************************************
     *                                    Utils
     ***********************************************************************************/

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    /* 更新 znode 内容 */
    public void updateZnode() {
        String path = "/rslist/" + rsName;
        StringBuffer znodeData = new StringBuffer(rsIp + ":" + externalPort + ":" + rsIp + ":" + internalPort);
        for (String tableName: masterTbls.keySet()) {  // 将所有主表写入 znode
            znodeData.append(":" + tableName);
        }
        while (true) {  // 不断尝试，直到更新成功
            try {
                zk.setData(path, znodeData.toString().getBytes(), zk.exists(path, true).getVersion());
                Thread.sleep(1000);
                System.out.println("Znode updated.");
                break;
            } catch (Exception e) {
                System.out.println("Failed to update znode.");
            }
        }
    }

    /***********************************************************************************
     *                                  程序入口                                         
     ***********************************************************************************/

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        RegionServer regionServer;
        Scanner in = new Scanner(System.in);
        String ip, port, internalPort, name;

        System.out.println("input your rs ip and port");
        System.out.print("ip:");
        ip = in.next();
        System.out.print("external port:");
        port = in.next();
        System.out.print("internal port: ");
        internalPort = in.next();
        System.out.println("input your rs name");
        name = in.next();
        in.close();

        regionServer = new RegionServer(ip, port, internalPort, name);

        regionServer.connect();
        regionServer.register();
        regionServer.run();
    }
}
