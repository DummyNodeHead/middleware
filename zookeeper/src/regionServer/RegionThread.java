package regionServer;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import miniSQL.API;
import miniSQL.Interpreter;
import miniSQL.QException;
import miniSQL.CATALOGMANAGER.Attribute;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.Table;
import miniSQL.INDEXMANAGER.Index;

/* 作为 region server，为 master 提供服务的线程 */
public class RegionThread extends Thread {
    RegionServer host;

    public RegionThread(RegionServer host) {
        this.host = host;
        System.out.println("New region thread created...");
    }

    @Override
    public void run() {
        while (true) {
            try {
                /* 如果线程被中断则释放资源并返回 */
                if (Thread.currentThread().isInterrupted()) {
                    host.setMasterSocket(null);
                    return;
                }
                /* 读取操作码并执行相应操作 */
                int op = host.readIntFromMaster();
                if (op == 0) {  // 表的备份操作
                    String tableName = host.readUTFFromMaster();
                    String[] ipAndPort = host.readUTFFromMaster().split(":");
                    Socket socket = new Socket(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
                    System.out.println("Save " + tableName + " to " + ipAndPort[0] + ":" + ipAndPort[1]);
                    API.store();
                    host.lock();
                    sendTableCatalog(tableName, socket);
                    sendTableFile(tableName, socket);
                    socket.close(); socket = new Socket(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
                    sendIndexFile(tableName + "_index", socket);
                    socket.close(); socket = new Socket(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
                    sendAssociatedIndexes(tableName, socket);
                    socket.close();
                    host.unlock();
                } else if (op == 1) {  // sql 语句
                    String command = host.readUTFFromMaster();
                    System.out.println("Received a command from master: " + command);
                    process(command);
                } else if (op == 2) {  // 将副表提升为主表
                    String tableName = host.readUTFFromMaster();
                    System.out.println("Promote " + tableName);
                    host.addReference(tableName);
                    host.updateZnode();
                }
            } catch (EOFException e) {  // 如果对方关闭了 socket 则中断线程
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void sendTableCatalog(String tableName, Socket socket) throws IOException {
        Table table = CatalogManager.getTable(tableName);
        StringBuffer catalog = new StringBuffer(table.primaryKey);

        for (Attribute attr: table.attributeVector) {
            catalog.append(" " + attr.attributeName);
            catalog.append(" " + attr.type.get_type());
            catalog.append(" " + attr.type.getLength());
            catalog.append(" " + (attr.isUnique ? "1" : "0"));
        }

        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(0);
        dos.writeUTF(tableName);
        dos.writeUTF(catalog.toString());
        System.out.println("Sent catalog: " + catalog);
    }

    private void sendTableFile(String tableName, Socket socket) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(1);
        dos.writeInt(CatalogManager.getTable(tableName).rowNum);
        BackupHandler.readAndSend(socket, tableName, "./" + tableName + ".table");
        try {
            Thread.sleep(1000);  // 等待一下再发后续内容，以免搞混
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Sent table file: " + "./" + tableName + ".table");
    }

    private void sendAssociatedIndexes(String tableName, Socket socket) throws IOException {
        Table table = CatalogManager.getTable(tableName);
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        for (Index index: table.indexVector) {
            if (index.attributeName == table.primaryKey) {
                continue;
            }
            dos.writeInt(2);
            dos.writeUTF(index.indexName);
            dos.writeUTF(tableName);
            dos.writeUTF(index.attributeName);
            System.out.println("Sent index catalog: " + index.indexName);
        }

    }

    private void sendIndexFile(String indexName, Socket socket) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(3);
        BackupHandler.readAndSend(socket, indexName, "./" + indexName + ".index");
        try {
            Thread.sleep(1000);  // 等待一下再发后续内容，以免搞混
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Sent index file: " + "./" + indexName + ".index");
    }

    private void sendGoodBye(Socket socket) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(4);
        System.out.println("Closed slave connection.");
    }

    private void process(String command) {
        System.out.println("Processing command from master: " + command);
        try {
            String result = Interpreter.interpretRemote(command);
            if (result.startsWith("-->Drop table")) {
                String tableName = result.split(" ")[2];
                host.dropMasterTable(tableName);
            }
            System.out.println(result);
        } catch (QException e) {
            System.out.println("Error code: " + e.type + ": " + e.msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
