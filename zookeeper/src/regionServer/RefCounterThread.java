package regionServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import miniSQL.API;

public class RefCounterThread extends Thread {
    private HashMap<String, Integer> refCount;
    private RegionServer host;

    public RefCounterThread(HashMap<String, Integer> refCount, RegionServer host) {
        this.refCount = refCount;
        this.host = host;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(60000);  // 暂定每 1 分钟发送一次
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            // 获取本机上主表的访问情况
            StringBuffer refInfo = new StringBuffer();
            for (Map.Entry entry: refCount.entrySet()) {
                refInfo.append(entry.getKey() + " " + entry.getValue() + " ");
                entry.setValue(0);
            }
    
            // 将访问情况发送给 master
            try {
                host.sendIntToMaster(1);  // 操作码为 1
                host.sendUTFToMaster(refInfo.substring(0, Math.max(refInfo.length() - 1, 0))); // 去掉最后一个空格
                API.store();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
