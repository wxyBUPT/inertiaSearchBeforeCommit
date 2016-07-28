package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class OriginalExtentManager {

    protected static Logger LOG = Logger.getLogger(OriginalExtentManager.class.getName());

    private static OriginalExtentManager originalExtentManager;

    /**
     * 当前没有被申请的extent 逻辑标号
     */
    private Integer currentExtentNo;

    /**
     * 单例模式
     */
    private OriginalExtentManager(){
        myExtentMap = new ConcurrentHashMap<>();
        currentExtentNo = 0;
    }

    public static synchronized OriginalExtentManager getInstance(){
        if(originalExtentManager==null){
            originalExtentManager = new OriginalExtentManager();
        }
        return originalExtentManager;
    }

    /**
     * 所有的extent
     */
    protected ConcurrentHashMap<Integer,OrigionExtent> myExtentMap;

    /**
     * 申请extent 逻辑标号
     * @return
     */
    public synchronized int applyExtentNo(){
        return currentExtentNo++;
    }

    public void putExtent(OrigionExtent extent){
        int n = extent.getExtentNo();
        myExtentMap.put(n,extent);
    }

    /**
     *
     */
    public Row getRowFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] rowByte = myExtentMap.get(_a).getBytesFromOfsAndSize(ofs,size);
        String line = new String(rowByte);
        return createKVMapFromLine(line);
    }

    private Row createKVMapFromLine(String line) {
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            RowKV kv = new RowKV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }
}
