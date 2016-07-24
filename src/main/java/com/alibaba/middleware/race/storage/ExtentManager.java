package com.alibaba.middleware.race.storage;


import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/23/16.
 * 管理磁盘所有的 extent
 */
public abstract class ExtentManager {

    protected static Logger LOG = Logger.getLogger(ExtentManager.class.getName());

    /**
     * 因为有三块磁盘,所以为了提高io,每个extentManager 维护三个当前文件
     * 可以确认如果对三个文件随机插入,三个文件有很大的可能被放在不同的磁盘
     */
    protected Vector<StoreFile> currentFiles;
    /**
     * 在三块磁盘中的Extent
     */
    protected Vector<Extent> currentExtents;

    protected FileManager fileManager;

    /**
     * 已知所有被创建的Extent
     */
    protected HashMap<Integer,Extent> extentMap;

    private Random random ;

    protected ExtentManager(){
        fileManager = FileManager.getInstance();
        /**
         * 创建存储文件,以及存储的Extent
         */
        currentFiles = new Vector<>(3);
        currentExtents = new Vector<>(3);
        extentMap = new HashMap<>();
        for(int i = 0;i<3;i++){
            StoreFile storeFile = newFile();
            currentFiles.add(storeFile);
            if(storeFile.hasNewExtent()){
                Extent extent = storeFile.getNewExtent();
                int extentNo = extent.getExtentNo();
                extentMap.put(extentNo,extent);
                currentExtents.add(extent);
            }
            else {
                System.out.println("Some bug happen, New file can't create extent");
                LOG.info("Some bug happen, New file can't create extent");
                System.exit(-1);
            }
        }
        random = new Random();
    }

    /**
     * 创建新的文件
     */
    abstract protected StoreFile newFile();

    /**
     * 调用次数较多的接口,向extent 中添加byte 数据
     * @param bytes 数据
     * @return
     */
    public DiskLoc putBytes(byte[] bytes){
        int nDisk = random.nextInt(3);
        /**
         * 向第 nDisk 个extent 中插入bytes 数据
         */
        DiskLoc diskLoc = currentExtents.get(nDisk).putBytes(bytes);
        /**
         * 如果成功插入,则直接返回
         */
        if(diskLoc!=null)return diskLoc;
        /**
         * 如果没有插入成功,则当前的extent 已经满了,获得新的extent
         * 如果当前文件已经没有空闲的extent ,则更新当前文件
         */
        if(!currentFiles.get(nDisk).hasNewExtent()){
            currentFiles.set(nDisk,newFile());
        }
        Extent extent = currentFiles.get(nDisk).getNewExtent();
        if(extent == null){
            System.out.println("ERROR");
            LOG.info("Some error happen, there is bug exist");
            System.exit(-1);
        }
        currentExtents.set(nDisk,extent);
        diskLoc = extent.putBytes(bytes);
        if(diskLoc ==null){
            System.out.println("ERROR");
            LOG.info("Some error happen, there is bug exist");
            System.exit(-1);
        }
        return diskLoc;
    }


    public byte[] getBytes(DiskLoc diskLoc){
        int _n = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        return extentMap.get(_n).getBytesFromOfsAndSize(ofs,size);
    }

    public byte[] getBytesForInsert(DiskLoc diskLoc){
        int _n = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        return extentMap.get(_n).getBytesFromOfsAndSizeForInsert(ofs,size);
    }
}
