package com.alibaba.middleware.race.storage;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 * DisLoc 存储的数据类型,包括原始数据,index 的叶子节点,index 的树节点
 */
public enum StoreType implements Serializable{
        ROWDATA,INDEXLEAFNODE,INDEXTREENODE,INDEXHEADER,INDEXNODE
}
