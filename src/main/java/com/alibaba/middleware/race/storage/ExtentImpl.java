package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/11/16.
 */
public class ExtentImpl implements Extent{

    //规定每个Extent 60M,
    private final static int EXTENTSIZE = 60 * 1024 * 1024;
    //Public Attributes
    DiskLoc firstRecord;
    DiskLoc lastRecord;
    int length;
    //这个Extent 的位置
    DiskLoc myLoc;
    Namespace nsDiagnostic;
    //下一个extent 的位置
    DiskLoc xnext;
    //上一个extent 的位置
    DiskLoc xprev;


    @Override
    public Extent getNextExtent() {

        return null;
    }

    @Override
    public Extent getPrevExtent() {
        return null;
    }

    @Override
    public Record getRecord(DiskLoc dl) {
        return null;
    }

    @Override
    public DiskLoc init(char[] nsname, int _length, int _fileNo, int _offset) {
        return null;
    }

    @Override
    public Record newRecord(int len) {
        return null;
    }
}
