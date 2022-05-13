package regionServer;

import java.io.*;
import java.net.Socket;

import miniSQL.Interpreter;
import miniSQL.QException;

/* 作为服务器，为单个 client 提供服务的线程 */
public class ServerThread extends Thread {
    
    private Socket socket;  // 客户端 socket
    private RegionServer host;
    private DataInputStream reader;
    private DataOutputStream writer;

    public ServerThread(Socket socket, RegionServer host) throws IOException {
        this.socket = socket;
        this.host = host;
        this.reader = new DataInputStream(socket.getInputStream());
        this.writer = new DataOutputStream(socket.getOutputStream());
        System.out.println("New server thread created... localhost:" + socket.getLocalPort() + " -> "
            + socket.getInetAddress().getHostName() + ":" + socket.getPort());
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                /* 如果线程被中断则释放资源并返回 */
                if (Thread.currentThread().isInterrupted()) {
                    socket.close();
                    return;
                }
                /* 如果 socket 失效则释放资源并返回 */
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    return;
                }
                /* 读取 sql 命令，执行并返回结果 */
                String command = reader.readUTF();
                System.out.println("Received a command from client: " + command);
                writer.writeUTF(process(command));
            } catch (EOFException e) {  // 如果对方关闭了 socket 则中断线程
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                // writer.writeUTF("Internel Server Error.\n");
                e.printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private String process(String command) throws InterruptedException {
        try {
            String tableName = command.split(" ")[0];  // 提取表名并增加其引用数
            host.addReference(tableName);
            
            String sql = command.substring(tableName.length() + 1);  // 提取 sql 语句并执行
            if (!sql.trim().startsWith("select")) {
                System.out.println(host.getMasterIp());
                System.out.println(host.getMasterPort());
                System.out.println(host.masterSocket);
                host.sendIntToMaster(0);  // 将指令发送给 master 以实现备份
                host.sendUTFToMaster(tableName);
                host.sendUTFToMaster(sql);
            }
            System.out.println("Processing command from client: " + sql);
            host.lock();
            String result = Interpreter.interpretRemote(sql);
            host.unlock();

            if (result.startsWith("-->Drop table")) {  // 删除表，需要更新本地缓存和 znode
                host.dropMasterTable(result.split(" ")[2]);
            }
            /* 创建表的请求只来自于 master，此处不考虑 */
            return result;
        } catch (EOFException e) {    // 如果对方关闭了 socket 则中断线程
            Thread.currentThread().interrupt();
            return "Socket closed.";
        } catch (QException e) {
            return "Error code: " + e.type + ": " + e.msg;
        } catch (IOException e) {
            e.printStackTrace();
            return "Internel server error.";
        }
    }
}
