package com.alibaba.middleware.race.storage;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/11/16.
 * 一个record 负责存储一个Row
 */
public interface Record {

    //获得下一个Record 的位置
    DiskLoc getNext();
    //获得前一个Record 的位置
    DiskLoc getPrev();
    Extent myExtent();

}
