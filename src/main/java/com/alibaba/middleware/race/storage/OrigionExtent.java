package com.alibaba.middleware.race.storage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/28/16.
 * 管理原始数据磁盘的最小的逻辑单位,使用MappedByteBuffer
 */
public class OrigionExtent {
    private static Logger LOG = Logger.getLogger(OrigionExtent.class.getName());

    /**
     * 所属文件信息
     */
    protected FileChannel fileChannel;
    protected MappedByteBuffer buffer;

    /**
     * 在文件中的位置,大小
     */
    Long position;

    /**
     * 用于记录文件当前的偏移量
     */

    /**
     * 用于记录自己是哪一个extent
     */
    int extentNo;

    /**
     * 初始化extent
     * @param fileChannel
     * @param position
     * @param size
     * @param extentNo
     */
    public OrigionExtent(FileChannel fileChannel,Long position,Long size,int extentNo){
        this.fileChannel = fileChannel;
        this.position = position;
        try{
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,position,size);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        this.extentNo = extentNo;
    }

    /**
     * 从对应位置的ofs 长度为size 的位置中获得byte 数据
     * @param ofs
     * @param size
     * @return
     */
    public synchronized byte[] getBytesFromOfsAndSize(int ofs,int size){
        byte[] bytes = new byte[size];
        buffer.position(ofs);
        buffer.get(bytes);
        return bytes;
    }

    /**
     * 从对应位置获得String
     * @param ofs
     * @param size
     * @return
     */
    public synchronized String getLineFromOfsAndSize(int ofs,int size){
        byte[] bytes = new byte[size];
        buffer.position(ofs);
        buffer.get(bytes);
        return new String(bytes);
    }

    public int getExtentNo(){
        return extentNo;
    }
}
