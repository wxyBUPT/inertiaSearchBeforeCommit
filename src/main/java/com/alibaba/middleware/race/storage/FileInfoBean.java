package com.alibaba.middleware.race.storage;

import java.nio.MappedByteBuffer;

/**
 * Created by xiyuanbupt on 7/19/16.
 */


public class FileInfoBean{
    private MappedByteBuffer mappedByteBuffer;
    private int fileNum ;
    public FileInfoBean(MappedByteBuffer mbf,int fn){
        this.fileNum = fn;
        this.mappedByteBuffer = mbf;
    }

    public MappedByteBuffer getBuffer(){
        return mappedByteBuffer;
    }
    public int getfileN(){
        return fileNum;
    }
}
