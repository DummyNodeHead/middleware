package master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.checkerframework.checker.units.qual.min;

public class MasterServer extends MasterInitializer {

    /* 开启连接 */
    public void startConnect() {
        client.start();

        try {
            /*
             * 在zk集群中插入表示master的节点
             * 查找master节点，看是否已经存在master
             * 如果不存在，则创建master节点
             */
            if (isNodeExist(masterPath)) {
                System.out.println("Master already exists!");
                return;
            }
            // 这里masterIp似乎不是很有必要，先保留一下
            client.create().withMode(CreateMode.EPHEMERAL).forPath(masterPath, "192.168.43.191:3333".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 开始监听
        Thread thread = new Thread(new HandleError());
        thread.start();

        // _______________________5.4 dzy新增_________________________________________//

        Thread client_connect = new Thread(new clientconnect());// 与client进行通信的线程
        client_connect.start();
        // 下面是接受来自region server的访问频率信息
        // Thread region_fre=new Thread(new fromregion());
        // region_fre.start();

        // _______________________5.4 dzy新增_________________________________________//

        // 循环监听不退出会话
        // 总觉得有除了死循环之外的更好的办法，但暂时没有找到
        while (true) {

        }
    }

    /* 关闭连接 */
    public void close() {
        client.close();
    }

    class clientconnect implements Runnable {
        ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();

        @Override
        public void run() {
            System.out.println("master等待client连接...");
            // 创建ServerSocket
            ServerSocket serverSocket;
            {
                try {
                    serverSocket = new ServerSocket(3333);
                    while (true) {

                        // System.out.println("线程信息 id =" + Thread.currentThread().getId() + " 名字=" +
                        // Thread.currentThread().getName());
                        // 监听，等待客户端连接
                        // System.out.println("等待连接....");
                        Socket socket = serverSocket.accept();
                        // System.out.println("连接到一个客户端");
                        System.out.println(
                                "Accepted a client: " + socket.getInetAddress().toString() + ":" + socket.getPort()
                                        + " local port: " + socket.getLocalPort());

                        // 就创建一个线程，与之通讯(单独写一个方法)
                        newCachedThreadPool.execute(new Runnable() {
                            public void run() { // 我们重写
                                // 可以和客户端通讯
                                handler(socket);
                            }
                        });
                    }
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            }
        }

        public void handler(Socket client) {
            try {
                while (client != null) {
                    DataInputStream dataInputStream = new DataInputStream(client.getInputStream());
                    // System.out.println("线程信息 id =" + Thread.currentThread().getId() + " 名字=" +
                    // Thread.currentThread().getName());
                    // System.out.println("read....");
                    String str = dataInputStream.readUTF();// str是接受到的消息
                    // dataInputStream.close();
                    // client.close();
                    System.out.println("from client(" + client.getInetAddress().toString() + ":" + client.getPort()
                            + " local port: " + client.getLocalPort() + "): " + str);
                    String tablename = str.split(" ")[0];// table name 是要创建的表的表名
                    str = str.substring(tablename.length() + 1);
                    // 下面要找到访问次数最少的Region server 把表名以及sql传给他

                    List<Map.Entry<String, Integer>> forSort = new ArrayList<>(
                            regionFreq.entrySet());
                    Collections.sort(forSort, ((o1, o2) -> o1.getValue() - o2.getValue())); // 从小到大排序
                    String min_region = forSort.get(0).getKey(); // 选出访问次数最少的 region server 所在的地址

                    // int min_count=0;
                    // Iterator<Map.Entry<String,Integer>>
                    // iterator=regionFreq.entrySet().iterator();
                    // if(iterator.hasNext()){
                    // Map.Entry<String,Integer> entry=iterator.next();
                    // min_region=entry.getKey();
                    // min_count=entry.getValue();
                    // }
                    // while(iterator.hasNext()){
                    // Map.Entry<String,Integer> entry=iterator.next();
                    // if(entry.getValue()<min_count){
                    // min_count=entry.getValue();
                    // min_region=entry.getKey();
                    // }
                    // }
                    // String ip=min_region.split(":")[0];
                    // int port=Integer.parseInt(min_region.split(":")[1]);
                    // 找到了最少的region server，发送socket
                    try {

                        // 这个是之前写的我单独创的socket
                        // client =new Socket(ip,port);//此时master相当于是这个region server的客户端
                        // 下面直接调用谦哥的socketList
                        Socket socket = socketList.get(min_region);
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeInt(1);
                        System.out.println("to min region address:" + min_region);
                        // dataOutputStream.writeUTF(tablename);//发送表名
                        // 发送给攀哥的格式："1 sql"
                        dataOutputStream.writeUTF(str);// 发送sql
                        System.out.println("to min region sql" + str);
                        // 上面是给主表所在的region发
                        // 下面是给副表所在的region发
                        String saddr = min_region;
                        // 下面找一个和主表所在地址不一样的region就行
                        Iterator<Map.Entry<String, List<String>>> iterator = regionTable.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, List<String>> entry = iterator.next();
                            if (!entry.getKey().equals(min_region)) {
                                saddr = entry.getKey();
                                break;
                            }
                        }
                        sTableAddr.put(tablename, saddr);// 创建副表的映射
                        System.out.println("saddr:" + saddr);
                        dataOutputStream = new DataOutputStream(socketList.get(saddr).getOutputStream());
                        dataOutputStream.writeInt(1);
                        // dataOutputStream.writeUTF(tablename);//发送表名
                        // 发送给攀哥的格式："1 tablename sql"
                        dataOutputStream.writeUTF(str);// 发送sql
                        System.out.println("to saddr:" + str);
                        regionTable.get(saddr).add(tablename);

                        // 提升为主表
                        dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeInt(2);
                        dataOutputStream.writeUTF(tablename);// 发送表名
                        System.out.println("ti sheng wei zhubiao" + tablename);

                        // dataInputStream=new
                        // DataInputStream(socket.getInputStream());//接受来自主表region的返回信息
                        // String hintfrommaddr=dataInputStream.readUTF();
                        // System.out.println("来自主表的region返回的提示信息："+hintfrommaddr);

                        // dataInputStream=new
                        // DataInputStream(socketList.get(saddr).getInputStream());//接受来自副表region的返回信息
                        // String hintfromsaddr=dataInputStream.readUTF();
                        // System.out.println("来自副表的region返回的提示信息："+hintfromsaddr);

                        // String createsuccess="";//根据攀哥的返回修改
                        // String createfailure="";
                        // if(hintfrommaddr.equals(hintfromsaddr)&&hintfrommaddr.equals(createsuccess)){
                        // dataOutputStream = new DataOutputStream(client.getOutputStream());
                        // dataOutputStream.writeUTF(createsuccess);
                        // System.out.println("发送给client创表成功信息："+createsuccess);
                        // }else {
                        // dataOutputStream = new DataOutputStream(client.getOutputStream());
                        // dataOutputStream.writeUTF(createfailure);
                        // System.out.println("发送给client创表失败信息："+createfailure);
                        // }

                    } catch (IOException exception) {
                        exception.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class HandleError implements Runnable {
        public void handler(Socket client) {
            InetAddress addr = client.getInetAddress();
            String ip = addr.getHostAddress();
            int port = client.getPort();
            String adder = ip + ":" + port;
            try {
                while (client != null) {
                    DataInputStream dataInputStream = new DataInputStream(client.getInputStream());
                    // System.out.println("线程信息 id =" + Thread.currentThread().getId() + " 名字=" +
                    // Thread.currentThread().getName());
                    // System.out.println("read....");

                    int type = dataInputStream.readInt();// 这边readint是用来区分region发给我的是频率还是sql
                    System.out.println("Received a request from region: " + client.getInetAddress().toString() + ":"
                            + client.getPort()
                            + " local port: " + client.getLocalPort());
                    System.out.println("type is " + type);
                    if (type == 1) {// 1代表频率
                        String str = dataInputStream.readUTF();// str是接受到的消息 格式统一为 tablename count tablename count...

                        // 关不关之后再说
                        // dataInputStream.close();
                        // client.close();
                        System.out.print("freq: ");
                        if (str.equals("")) {
                            System.out.println("(Empty)");
                        } else {
                            System.out.println(str);
                            String[] mess = str.split(" ");
                            int countsum = 0;// 每个region的访问次数
                            for (int i = 0; i < mess.length; i += 2) {
                                tableFreq.put(mess[i], Integer.parseInt(mess[i + 1]));// 修改table的访问频率
                                countsum += Integer.parseInt(mess[i + 1]);
                            }
                            regionFreq.put(adder, countsum);// 修改region的访问频率
                            System.out.println("Changed freqency to " + countsum + " for region at " + addr);
                            balance();// 用于处理负载均衡
                        }
                    }
                    if (type == 0) {// 接受来自region的需要改变表内容的请求
                        // 下面要区分是创建从表的请求还是修改从表的请求
                        // 创建从表的话需要找一个region server给它
                        // 修改从表的话再stable这个map里已经存放了从表的region所在地址了
                        System.out.println(client.getLocalPort() + "cao");
                        String table = dataInputStream.readUTF();
                        System.out.println("Table: " + table);
                        String sql = dataInputStream.readUTF();
                        System.out.println("Sql: " + sql);
                        System.out.println("from region server(" + client.getInetAddress().toString() + ":" +
                                client.getPort() + " local port: " + client.getLocalPort() + "): " + table + ": "
                                + sql);
                        // if(sql.startsWith("create")){
                        // 这部分就写在与client的通信那边了
                        // String maddr=mTableAddr.get(table);//主表所在地址
                        // String saddr=maddr;
                        // //下面找一个和主表所在地址不一样的region就行
                        // Iterator<Map.Entry<String,List<String>>>
                        // iterator=regionTable.entrySet().iterator();
                        // while (iterator.hasNext()){
                        // Map.Entry<String,List<String>> entry=iterator.next();
                        // if(!entry.getValue().contains(table)){
                        // saddr=entry.getKey();
                        // break;
                        // }
                        // }
                        // sTableAddr.put(table,adder);//创建副表的映射
                        // DataOutputStream dataOutputStream = new
                        // DataOutputStream(socketList.get(saddr).getOutputStream());
                        // dataOutputStream.writeInt(1);
                        // //dataOutputStream.writeUTF(tablename);//发送表名
                        // //发送给攀哥的格式："1 sql"
                        // dataOutputStream.writeUTF(table+" "+sql);//发送sql

                        // }//else下面就是已知副表在哪里的情况下进行发送sql
                        // else{
                        System.out.println("change stable:");
                        String saddr = sTableAddr.get(table);
                        DataOutputStream dataOutputStream = new DataOutputStream(
                                socketList.get(saddr).getOutputStream());
                        dataOutputStream.writeInt(1);
                        // dataOutputStream.writeUTF(tablename);//发送表名
                        // 发送给攀哥的格式："1 sql"
                        dataOutputStream.writeUTF(sql);// 发送sql
                        System.out.println("to region sql:" + sql);
                        // }
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void balance() {
            List<Map.Entry<String, Integer>> forminSort = new ArrayList<>(
                    regionFreq.entrySet());
            Collections.sort(forminSort, ((o1, o2) -> o1.getValue() - o2.getValue())); // 从小到大排序
            String min_region = forminSort.get(0).getKey(); // 选出访问次数最少的 region server 所在的地址
            System.out.println("min_region "+min_region);
            List<Map.Entry<String, Integer>> formaxSort = new ArrayList<>(
                    regionFreq.entrySet());
            Collections.sort(formaxSort, ((o1, o2) -> o2.getValue() - o1.getValue())); // 从大到小排序
            String max_region = formaxSort.get(0).getKey(); // 选出访问次数最多的 region server 所在的地址
            System.out.println("max_region "+max_region);
            if(min_region.equals(max_region))
                return;
            if(formaxSort.size()==2)
                return;
        
        
            int max_count = formaxSort.get(0).getValue();
            if (max_count > Access_threshold) {// 如果大于访问阈值，进行移动
                ArrayList list = (ArrayList) regionTable.get(max_region);
                String max_table = (String) list.get(0);// 找到要移动的表
                int max_tablecount = tableFreq.get(list.get(0));
                for (int i = 0; i < list.size(); i++) {
                    if (tableFreq.get(list.get(i)) > max_tablecount) {
                        max_tablecount = tableFreq.get(list.get(i));
                        max_table = (String) list.get(i);
                    }
                }
                forminSort = new ArrayList<>(
                        regionFreq.entrySet());
                Collections.sort(forminSort, ((o1, o2) -> o1.getValue() - o2.getValue())); // 从小到大排序
                if(sTableAddr.get(max_table).equals(min_region))
                    min_region=forminSort.get(1).getKey();
        
        
                // 下面分三步，首先发给最少的region进行备份
                // 然后给最小表所在的region发删表sql
                // 最后给最少的region发将table提升为主表的信息
                DataOutputStream dataOutputStream = null;
                try {
                    dataOutputStream = new DataOutputStream(socketList.get(max_region).getOutputStream());
                    dataOutputStream.writeInt(0);
                    dataOutputStream.writeUTF(max_table);// 发送表名
                    dataOutputStream.writeUTF(min_region);// 发送从表主机地址
                    System.out.println("发送给max_region：" + max_table + " " + min_region);
        
                    dataOutputStream = new DataOutputStream(socketList.get(max_region).getOutputStream());
                    dataOutputStream.writeInt(1);
                    String sql = "drop table " + max_table + ";;";
                    dataOutputStream.writeUTF(sql);// 发送sql
                    System.out.println("发给max region 删表信息" + sql);
        
                    dataOutputStream = new DataOutputStream(socketList.get(min_region).getOutputStream());
                    dataOutputStream.writeInt(2);
                    dataOutputStream.writeUTF(max_table);// 发送表名
                    System.out.println("移动表的时候发的提升主表信息");
                    
                    // mTableAddr.put(max_table, min_region);

                    // list.remove(max_table);
                    // ArrayList list1 = (ArrayList) regionTable.get(min_region);
                    // list1.add(max_table);
                    // regionTable.put(max_region, list);
                    // regionTable.put(min_region, list1);
                

                    Iterator<Map.Entry<String, Integer>> iterator = regionFreq.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Integer> entry = iterator.next();
                        regionFreq.put(entry.getKey(), 0);
                    }
                    iterator = tableFreq.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Integer> entry = iterator.next();
                        tableFreq.put(entry.getKey(), 0);
                    }
        
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
        
            }
        }

        @Override
        public void run() {
            CuratorCache curatorCache = CuratorCache.builder(client, "/rslist").build(); // 监听 rslist 节点下的所有子节点
            CuratorCacheListener listener = CuratorCacheListener.builder().forChanges((oldNode, node) -> {

                /* region server 发生变化 */
                System.out.println("节点 " + node.getPath() + " 发生变化");
                // 需要判断是新增了主表还是删除了主表
                // 转发 sql 语句不是这个线程做的，而是监听 region server 的线程做的
                // 这个方法主要需要更新几个 map
                String[] oldData = new String(oldNode.getData()).split(":");
                String[] curData = new String(node.getData()).split(":");
                Set<String> findSet = new HashSet<String>();
                List<String> tableList = new ArrayList<String>();
                if (oldData.length > curData.length) {
                    // 说明有表被删除了
                    for (int i = 4; i < curData.length; ++i) {
                        findSet.add(curData[i]);
                        tableList.add(curData[i]);
                    }
                    // 更新 regionTable
                    regionTable.put(curData[2] + ":" + curData[3], tableList);
                    for (int i = 4; i < oldData.length; ++i) {
                        if (findSet.contains(oldData[i]) == false) {
                            // 更新 mTableAddr 和 tableFreq
                            if (mTableAddr.get(oldData[i]).equals(oldData[0] + ":" + oldData[1])) {
                                mTableAddr.remove(oldData[i]);
                                tableFreq.remove(oldData[i]);
                            }
                            // sTableAddr 我这里就不删了，因为监听线程应该要用到
                            break;
                        }
                    }
                } else {
                    // 说明有表新增
                    for (int i = 4; i < oldData.length; ++i)
                        findSet.add(oldData[i]);
                    for (int i = 4; i < curData.length; ++i) {
                        tableList.add(curData[i]);
                        if (findSet.contains(curData[i]) == false) {
                            // 更新 mTableAddr 和 tableFreq
                            System.out.println("收到" + curData[2] + ":" + curData[3]);
                            mTableAddr.put(curData[i], curData[2] + ":" + curData[3]);
                            System.out
                                    .println("Master of table " + curData[i] + " is: " + curData[2] + ":" + curData[3]);
                            tableFreq.put(curData[i], 0);
                            // 同样 sTableAddr 我这里也不更新了，应该要负载均衡那里更新
                        }
                    }
                    // 更新 regionTable
                    regionTable.put(curData[2] + ":" + curData[3], tableList);
                }

            }).
                    forCreates(node -> {

                /* 有新的 region server 上线 */
                System.out.println("有新的 region server 上线");
                String[] arr = new String(node.getData()).split(":");
                String addr = arr[2] + ":" + arr[3];
                try {
                    Socket socket = new Socket(arr[2], Integer.parseInt(arr[3])); // 建立 socket 连接
                    ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
                    System.out.println(
                            "Accepted region server: " + socket.getInetAddress().toString() + ":" + socket.getPort()
                                    + " local port: " + socket.getLocalPort());
                    // 就创建一个线程接受来自region的消息
                    newCachedThreadPool.execute(new Runnable() {
                        public void run() { // 重写
                            // 可以和客户端通讯
                            handler(socket);
                        }
                    });

                    socketList.put(addr, socket);
                    List<String> tableList = new ArrayList<String>();
                    // 当上线的 region 不是第一个时
                    if (socketList.size() > 1) {
                        // 将所有的主表备份给当前访问次数最少的 region server
                        List<Map.Entry<String, Integer>> forSort = new ArrayList<>(
                                regionFreq.entrySet());
                        Collections.sort(forSort, ((o1, o2) -> o1.getValue() - o2.getValue())); // 从小到大排序
                        String ipAndport = forSort.get(0).getKey(); // 选出访问次数最少的 region server 所在的地址
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        for (int i = 4; i < arr.length; ++i) {
                            System.out.println("Backup " + arr[i] + ".");
                            tableList.add(arr[i]);
                            tableFreq.put(arr[i], 0);
                            mTableAddr.put(arr[i], addr);
                            sTableAddr.put(arr[i], ipAndport); // 这里应该不冲突，我这里也在 sTableAddr 中加入
                            System.out.println("need to send message");
                            dos.writeInt(0);
                            dos.writeUTF(arr[i]);
                            dos.writeUTF(ipAndport);
                            dos.flush();
                        }
                    }
                    regionTable.put(addr, tableList);
                    regionFreq.put(addr, 0);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).
                    forDeletes(oldNode -> {

                /**
                 * 容错容灾
                 * 实际上就是处理某个 region server 宕机的情况
                 */
                System.out.println("有 region server 宕机了");
                String[] arr = new String(oldNode.getData()).split(":");
                String addr = arr[2] + ":" + arr[3];
                System.out.println("the addr is " + addr);
                List<String> tableList = regionTable.get(addr);
                System.out.print("Tables: ");
                for (String table : tableList)
                    System.out.println(table);
                regionTable.remove(addr);
                regionFreq.remove(addr);
                socketList.remove(addr);
                List<Map.Entry<String, Integer>> forSort = new ArrayList<>(
                        regionFreq.entrySet());
                Collections.sort(forSort, ((o1, o2) -> o1.getValue() - o2.getValue())); // 从小到大排序
                String ipAndport = forSort.get(0).getKey(); // 选出访问次数最少的 region server 所在的地址
                String ipAndPort1 = forSort.get(1).getKey(); // 第二小的
                System.out.println("target region server is" + ipAndport);
                try {
                    for (String table : tableList) {
                        /* 处理主表 */
                        System.out.println("mTables:");
                        for (Map.Entry entry: mTableAddr.entrySet()) {
                            System.out.println(entry.getKey());
                        }
                        if (mTableAddr.get(table).equals(addr)) {
                            String slv_addr = sTableAddr.get(table), new_slv = ipAndport;
                            if (slv_addr.equals(new_slv)) {
                                new_slv = ipAndPort1;
                            }
                            mTableAddr.put(table, slv_addr);// 更新 mTableAddr
                            sTableAddr.put(table, new_slv);// 更新 sTableAddr
                            regionTable.get(new_slv).add(table);// 更新从表所在 region 的 region table
                            System.out.println("chuli main");
                            System.out.println("New master of table " + table + " is: " + slv_addr);
                            System.out.println("New slave of table " + table + " is: " + new_slv);
                            Socket socket = socketList.get(slv_addr);
                            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                            // 0 表示备份
                            dos.writeInt(0);
                            dos.writeUTF(table);
                            dos.writeUTF(new_slv);
                            // 2 表示提升为主表
                            dos.writeInt(2);
                            dos.writeUTF(table);
                            dos.flush();
                        } else {
                            String mst_addr = mTableAddr.get(table), new_slv = ipAndport;
                            if (new_slv.equals(mst_addr)) {
                                new_slv = ipAndPort1;
                            }
                            System.out.println("chuli slave");
                            System.out.println("New slave of table " + table + "is: " + new_slv);
                            sTableAddr.put(table, new_slv);
                            regionTable.get(new_slv).add(table);
                            Socket socket = socketList.get(mst_addr);
                            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                            dos.writeInt(0);
                            dos.writeUTF(table);
                            dos.writeUTF(new_slv);
                            dos.flush();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).forInitialized(() -> System.out.println("Master 开始监听了...")).build();
            curatorCache.listenable().addListener(listener);
            curatorCache.start();
        }

    }

    class HandleLoad implements Runnable {

        @Override
        public void run() {

        }
    }

    // 测试
    public static void main(String[] args) {
        MasterServer masterServer = new MasterServer();
        // Scanner in = new Scanner(System.in);
        // String ip, port;

        // System.out.println("input your rs ip and port");
        // System.out.print("ip:");
        // ip = in.next();
        // System.out.print("external port:");
        // port = in.next();
        // in.close();
        masterServer.startConnect();
    }
}
