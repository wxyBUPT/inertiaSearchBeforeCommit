package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
public interface WriteAble {
    public DiskLoc writeRow(Row row);
}
