package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public interface Extent {
    abstract public Extent getNextExtent();
    abstract public Extent getPrevExtent();
    abstract public Record getRecord(DiskLoc dl);
    abstract DiskLoc init(char[] nsname, int _length, int _fileNo, int _offset);
    abstract Record newRecord(int len);

}
