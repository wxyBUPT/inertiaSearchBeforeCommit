package com.alibaba.middleware.race.storage;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class IndexLeafNode<T extends Serializable> extends IndexNode<T>{

    @Override
    IndexNode insert(T t) {
        return null;
    }

    @Override
    DiskLoc search(T t) {
        return null;
    }
}
