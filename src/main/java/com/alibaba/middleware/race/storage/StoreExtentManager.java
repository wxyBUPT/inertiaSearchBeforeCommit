package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.codec.RowCreator;
import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;

/**
 * Created by xiyuanbupt on 7/23/16.
 *
 */
public class StoreExtentManager extends ExtentManager{

    private static StoreExtentManager storeExtentManager;

    /**
     * 子类做成单例模式
     */
    private StoreExtentManager(){
        super();
    }

    public static synchronized StoreExtentManager getInstance(){
        if(storeExtentManager==null){
            storeExtentManager = new StoreExtentManager();
        }
        return storeExtentManager;
    }

    @Override
    protected StoreFile newFile() {
        return fileManager.createNewStoreFile();
    }

    /**
     * 从磁盘位置获得Row,可能最浪费时间的代码就在这段
     * 这段代码需要在另外的位置重构
     * @param diskLoc
     * @return
     */
    public Row getRowFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] rowByte = extentMap.get(_a).getBytesFromOfsAndSize(ofs,size);
        String line = new String(rowByte);
        return RowCreator.createKVMapFromLine(line);
    }

    public String getLineFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] rowByte = extentMap.get(_a).getBytesFromOfsAndSize(ofs,size);
        return new String(rowByte);
    }

    public Row getRowFromDiskLocForInsert(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] rowByte = extentMap.get(_a).getBytesFromOfsAndSizeForInsert(ofs,size);
        String line = new String(rowByte);
        return RowCreator.createKVMapFromLine(line);
    }
}
