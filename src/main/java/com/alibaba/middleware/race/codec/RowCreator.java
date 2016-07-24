package com.alibaba.middleware.race.codec;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;

/**
 * Created by xiyuanbupt on 7/24/16.
 * 从String 中产生Row 格式数据
 */
public class RowCreator {

    public static Row createKVMapFromLine(String line){
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            RowKV kv = new RowKV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }
}
