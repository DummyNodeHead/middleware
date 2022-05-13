package regionServer;

import java.util.*;
import java.io.*;
import java.net.*;

/* 管理面向 client 提供服务所需的资源 */
public class ExternalServer implements Runnable {
    
    private String ip, port;
    private RegionServer host;

    private ServerSocket serverSocket;  // 监听端口 socket

    private HashMap<String, Thread> serverThreads;  // 客户端通信线程池

    public ExternalServer(String ip, String port, RegionServer host) throws IOException {
        this.ip = ip;
        this.port = port;
        this.host = host;

        this.serverSocket = new ServerSocket(Integer.parseInt(port));
        this.serverThreads = new HashMap<>();
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                String addr = socket.getInetAddress().toString() + ":" + socket.getPort();
                Thread thread = new ServerThread(socket, host);
                
                if (serverThreads.get(addr) != null) {  // 中断旧线程，防止资源泄漏
                    serverThreads.get(addr).interrupt();
                }
                // 将新线程加入线程池并启动
                serverThreads.put(addr, thread);
                thread.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
