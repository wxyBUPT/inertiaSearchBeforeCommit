package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/11/16.
 */
public class RecordImpl implements Record{

    //数据头的长度
    public static final int HEADERSIZE = 16;

    //在所属extent 中的偏移位置
    public int extentOfs;
    //包括数据,与数据头的总的数据长度
    public int lengthWithHeaders;
    //下一个Extent 的位置
    public int nextOfs;
    //上一个Extent 的位置
    public int prevOfs;

    @Override
    public DiskLoc getNext() {
        return null;
    }

    @Override
    public DiskLoc getPrev() {
        return null;
    }

    @Override
    public Extent myExtent() {
        return null;
    }
}
