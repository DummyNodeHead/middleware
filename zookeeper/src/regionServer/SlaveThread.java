package regionServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import miniSQL.API;
import miniSQL.Interpreter;
import miniSQL.QException;
import miniSQL.CATALOGMANAGER.CatalogManager;

/* 作为从表主机，为单个主表主机提供服务的线程 */
public class SlaveThread extends Thread {
    Socket socket;
    RegionServer host;
    DataInputStream reader;
    DataOutputStream writer;

    public SlaveThread(Socket socket, RegionServer host) throws IOException {
        this.socket = socket;
        this.host = host;
        this.reader = new DataInputStream(socket.getInputStream());
        this.writer = new DataOutputStream(socket.getOutputStream());
        System.out.println("New slave thread created... localhost:" + socket.getLocalPort() + " -> "
            + socket.getInetAddress().getHostName() + ":" + socket.getPort());
    }

    @Override
    public void run() {
        while (true) {
            try {
                /* 如果线程被中断则释放资源并返回 */
                if (Thread.currentThread().isInterrupted()) {
                    socket.close();
                    return;
                }
                /* 读取操作码并执行相应操作 */
                int op = reader.readInt();
                if (op == 0) {  // 表头信息
                    String tableName = reader.readUTF();  // 先读取表名
                    String[] attributes = reader.readUTF().split(" "); // 再读取表的属性
                    for (String s : attributes) {
                        System.out.println("---" + s);
                    }
                    StringBuffer sql = new StringBuffer("create table " + tableName + "(");  // 用 miniSQL 语句创建表
                    // 构造 sql 语句
                    for (int i = 1; i < attributes.length; i += 4) {  // 从 1 开始遍历，attributes[0] 是主键
                        String attrName = attributes[i];
                        String type = attributes[i + 1].toLowerCase();
                        String length = (type.equals("char") ? "(" + attributes[i + 2] + ")" : "");
                        String isUnique = (attributes[i + 3].equals("1") ? " unique" : "");
                        
                        sql.append(attrName + " " + type + length + isUnique + ", ");
                    }
                    sql.append("primary key(" + attributes[0] + "));;");
                    
                    System.out.println("Received catalog for table [" + tableName + "].");
                    System.out.println(sql.toString());
                    try {
                        Interpreter.interpretRemote(sql.toString());  // 执行 sql 语句
                        // TODO: 向主表主机发送消息，提示成功
                    } catch (QException e) {
                        System.out.println(e.msg);
                        // TODO: 向主表主机发送消息，提示错误
                    }
                } else if (op == 1) {  // 表文件备份
                    int rowNum = reader.readInt(); // 读取表的记录数量
                    System.out.println("row num: " + rowNum);
                    String tableName = reader.readUTF();    // 先读取表名
                    byte[] buffer = reader.readAllBytes();  // 再读取文件数据

                    CatalogManager.setRowNum(tableName, rowNum);  // 先更新记录数量
                    try {
                        API.store();  // 再将缓存区内容写入磁盘
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    host.lock();
                    BackupHandler.saveFile("./" + tableName + ".table", buffer);  // 最后保存文件，保证更新不会被覆盖
                    host.unlock();

                    System.out.println("Received data for table [" + tableName + "].");
                    // TODO: 向主表主机发送消息，提示成功
                } else if (op == 2) {  // 索引信息
                    String indexName = reader.readUTF();
                    String tableName = reader.readUTF();
                    String attrName  = reader.readUTF();
                    String sql = "create index " + indexName + " on " + tableName + "(" + attrName + ");;";

                    System.out.println("Received catalog for index [" + indexName + "].");
                    System.out.println(sql);
                    try {
                        Interpreter.interpretRemote(sql.toString());
                        // TODO: 向主表主机发送消息，提示成功
                    } catch (QException e) {
                        System.out.println(e.msg);
                        // TODO: 向主表主机发送消息，提示错误
                    }
                } else if (op == 3) {
                    String indexName = reader.readUTF();
                    byte[] buffer = reader.readAllBytes();
                    try {
                        API.store();  // 再将缓存区内容写入磁盘
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    host.lock();
                    BackupHandler.saveFile("./" + indexName + ".index", buffer);  // 最后保存文件，保证更新不会被覆盖
                    host.unlock();

                    System.out.println("Received data for index [" + indexName + "].");
                }
                // } else if (op == 4) {  // 断开连接
                //     socket.close();
                //     return;
                // }
            } catch (EOFException e) {  // 如果对方关闭了 socket 则中断线程
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
