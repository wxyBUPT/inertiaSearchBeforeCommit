package com.alibaba.middleware.race.models;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/9/16.
 * 一个Extent 一个物理磁盘页
 * Extent 是存储在磁盘上的,应该支持序列化
 */
public class Extent implements Serializable{

    private final static int DISKSIZE = 512;//512B

    //八字节,标识下一个extent 和上一个extent
    private final byte[] nextExtent;
    private final byte[] preExtent;

    //四字节,标识第一个,和最后一个record的位置
    private final byte[] firstRecord;
    private final byte[] lastRecord;

    //四字节,标识header 的大小
    private final byte[] headerSize;

    public Extent(byte[] nextExtent,byte[] preExtent,byte[] firstRecord,byte[] lastRecord,byte[] headerSize){
        this.nextExtent = nextExtent;
        this.firstRecord = firstRecord;
        this.preExtent = preExtent;
        this.lastRecord = lastRecord;
        this.headerSize = headerSize;
    }

    
}