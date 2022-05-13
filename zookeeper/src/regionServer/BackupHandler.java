package regionServer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用方法（以下编号不表示顺序）
 * 1. 实例化。BackupHandler bh = new BackupHandler(6666)
 * 2. 开启监听。bh.startListen()
 * 3. 读取路径url下的文件并发送给addr。bh.readAndSend(addr,tableName,url)
 * 这个tableName是用来告诉接收方该保存到哪个文件里的。
 * 详细参数可以看后面函数的注释。
 */

public class BackupHandler {
    private int serverPort;
    private ServerSocket serverSocket;

    /**
     * 构造函数，主要是构造接收的端口
     *
     * @param _serverPort
     */
    BackupHandler(int _serverPort) throws IOException {
        serverPort = _serverPort;
        serverSocket = new ServerSocket(_serverPort);
    }

    public class Listener implements Runnable {
        @Override
        public void run() {
            try {
                listen();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 调用这个函数，开启新线程进行监听
     */
    public void startListen() {
        Listener ls = new Listener();
        Thread listener = new Thread(ls);
        listener.start();
    }

    /**
     * 监听并接收发送来的备份数据，
     * 同时进行备份保存。
     * <p>
     * 注意：调用该函数时需要catch异常。
     *
     * @throws IOException
     */
    public void listen() throws IOException {
        // 线程池
        ExecutorService pool = Executors.newCachedThreadPool();

        while (true) {
            Socket socket = serverSocket.accept();
            /*
              监听线程核心逻辑
              在外面套个if即可
             */
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        // if(是走备份逻辑){
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        String tableName = in.readUTF();  // 先读取表名
                        byte[] buffer = in.readAllBytes();  // 再读取文件数据
                        saveFile("./test/" + tableName, buffer);    // 保存文件
                        // }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    /**
     * 发送buffer至目标regionServer
     *
     * @param buffer
     * @param addr      是xxx.xxx.xxx.xxx:xxxx的形式
     * @param tableName 表名
     */
    public static void send(byte[] buffer, String addr, String tableName) {
        try {
            // 根据:分出ip和地址，然后建立socket连接
            String[] ipAndPort = addr.split(":");
            Socket socket = new Socket(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
            // 获取输出流进行输出
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(tableName);    // 先告知接收方这是哪个表，不然接收方不知道要存在哪
            out.write(buffer);      // 再发送表的内容
            out.flush();

            // 关闭socket与输出流
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * send 函数的重载，将第二个参数 addr 改为对应的目标 socket
     */
    public static void send(byte[] buffer, Socket socket, String tableName) {
        try {
            // 获取输出流进行输出
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(tableName);    // 先告知接收方这是哪个表，不然接收方不知道要存在哪
            out.write(buffer);      // 再发送表的内容
            out.flush();

            // 关闭socket与输出流
            // out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 把buffer中的数据保存至url的文件中
     *
     * @param url
     * @param buffer
     */
    public static void saveFile(String url, byte[] buffer) {
        File file = new File(url);
        try {
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(buffer);
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从url的文件中读取byte[]
     *
     * @param url
     */
    public static byte[] readFile(String url) {
        File file = new File(url);

        try {
            // 若文件不存在，抛出异常
            if (!file.exists()) {
                throw new FileNotFoundException();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        byte[] buffer = new byte[(int) file.length()];
        FileInputStream in = null;

        try {
            in = new FileInputStream(file);
            // 要读的长度
            int toRead = in.available();
            in.read(buffer, 0, toRead);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return buffer;
    }

    /**
     * 读取并发送数据，是send和readFile的封装
     *
     * @param addr      接收方地址，是如"127.0.0.1:6666"的字符串
     * @param tableName 这次发送的表名。主要是为了让接收方知道该存到哪
     * @param url       在本机上应该去哪读取这个表的文件
     */
    public static void readAndSend(String addr, String tableName, String url) {
        byte[] buffer = readFile(url);
        send(buffer, addr, tableName);
    }

    public static void readAndSend(Socket socket, String tableName, String url) {
        byte[] buffer = readFile(url);
        send(buffer, socket, tableName);
    }

}

/*
 使用例子
 监听
 public class Test {
    public static void main(String[] args) {
        BackupHandler bh = new BackupHandler(6666);
        bh.startListen();
    }
 }
 发送
 bh.readAndSend("127.0.0.1:1234","hello","./test/test");
 */