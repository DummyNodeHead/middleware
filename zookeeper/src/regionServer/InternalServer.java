package regionServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/* 管理面向 master 和主表提供服务所需的资源 */
public class InternalServer implements Runnable {

    private String ip, port;
    private RegionServer host;

    private ServerSocket serverSocket;  // 监听端口 socket

    private HashMap<String, Thread> slaveThreads;  // 主表主机通信线程池
    
    private RegionThread regionThread;  // master 通信线程

    public InternalServer(String ip, String port, RegionServer host) throws IOException {
        this.ip = ip;
        this.port = port;
        this.host = host;
        
        this.serverSocket = new ServerSocket(Integer.parseInt(port));
        this.slaveThreads = new HashMap<>();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                String ip = socket.getInetAddress().toString().substring(1);
                int port = socket.getPort();
                String addr = ip + ":" + port;
                
                System.out.println(host.getMasterIp());
                System.out.println(ip);
                if (ip.equals(host.getMasterIp())) {  // 连接的是 master，用 master 线程处理
                    host.setMasterSocket(socket);
                    regionThread = new RegionThread(host);
                    regionThread.start();
                } else {  // 连接的是其他 region server，用从表线程处理
                    Thread thread = new SlaveThread(socket, host);

                    if (slaveThreads.get(addr) != null) {  // 中断旧线程，防止资源泄漏
                        slaveThreads.get(addr).interrupt();
                    }
                    // 将新线程加入线程池并启动
                    slaveThreads.put(addr, thread);
                    thread.start();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
