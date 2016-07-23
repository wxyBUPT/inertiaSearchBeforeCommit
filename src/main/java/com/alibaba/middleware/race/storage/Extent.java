package com.alibaba.middleware.race.storage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/22/16.
 * 管理磁盘最小的单位,使用MappedByteBuffer
 * 在内存中维护这组Extent 信息
 */
public class Extent {

    private static Logger LOG = Logger.getLogger(Extent.class.getName());

    /**
     * 所属文件的信息
     */
    protected FileChannel fileChannel ;
    protected MappedByteBuffer buffer;

    /**
     * 在文件中的位置,和大小
     */
    //记录position 的目的是将位置信息保存到恢复为只读模式的时候
    Long position;
    Integer size;

    /**
     * 用于记录文件当前偏移量
     */
    Integer currentOfs;

    /**
     * 用于记录自己是哪一个extent
     */
    int extentNo;

    /**
     * 所有初始化动作交给extent 的构造函数
     * 并且Extent 设计为需要根据构造函数知道自己位置,文件信息等内容
     * @param fileChannel 所属文件的通道
     * @param position 在所属文件的位置
     * @param size buffer 的大小
     * @param extentNo extent 的逻辑号码
     */
    public Extent(FileChannel fileChannel,Long position,Long size,int extentNo){
        this.fileChannel = fileChannel;
        this.position = position;
        try {
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        this.extentNo = extentNo;
        this.size = size.intValue();
        currentOfs = 0;
    }

    /**
     * 用于完成文件创建显示的执行数据同步
     */
    public synchronized void finishConstruct(){
        buffer.force();
    }

    public synchronized void makeReadOnly(FileChannel fileChannel){
        this.fileChannel = fileChannel;
        try {
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size.longValue());
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * 从对应位置ofs 长度为 size 的位置中获得Row 格式的数据
     * @param ofs 从这个extent 的 ofs 位置获取
     * @param size 获取的数据长度为 size
     * @return
     */
    public synchronized byte[] getBytesFromOfsAndSize(int ofs, int size){
        byte[] bytes = new byte[size];
        buffer.position(ofs);
        buffer.get(bytes);
        return bytes;
    }

    /**
     * 如果之后还要想这个extent中插入数据则使用下面的方法,下面的方法目前没有作用
     * @param ofs  相对便宜位置
     * @param size 获得数据大小
     * @return
     */
    public synchronized byte[] getBytesFromOfsAndSizeForInsert(int ofs,int size){
        byte[] bytes = new byte[size];
        buffer.position(ofs);
        buffer.get(bytes);
        buffer.position(currentOfs);
        return bytes;
    }

    /**
     * 向Extent 中添加bytes 数据,如果添加成功,返回 extent 的byteSize,如果添加不成功,则返回null
     * 插入的数据类型为notdefined
     * @param bytes 插入的bytes 数据
     * @return
     */
    public synchronized DiskLoc putBytes(byte[] bytes){
        int size = bytes.length;
        if(getRemainSize()<size){
            return null;
        }
        buffer.put(bytes);
        DiskLoc diskLoc = new DiskLoc(extentNo,currentOfs,StoreType.NOTDEFINED,size);
        //update currentOfs
        currentOfs = buffer.position();
        return diskLoc;
    }

    public synchronized int getRemainSize(){
        //每一个Extent 预留4bytes 的数据
        return size - buffer.position();
    }

    public int getExtentNo(){
        return extentNo;
    }


}
