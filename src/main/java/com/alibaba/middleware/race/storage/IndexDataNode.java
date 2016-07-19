package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;

import java.io.Serializable;
import java.util.List;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
public class IndexDataNode implements Serializable,Comparable<IndexDataNode> {
    //相当于存储的key值
    List<Comparable> keys;

    //存储磁盘位置
    DiskLoc diskLoc;

    IndexDataNode(List<Comparable> keys,DiskLoc diskLoc){
        this.keys = keys;
        this.diskLoc = diskLoc;
    }

    @Override
    public int compareTo(IndexDataNode o) {
        if(this.keys.size() != o.keys.size()){
            throw new RuntimeException("Bad ordering keys, there is a bug maybe");
        }
        for(int i = 0;i<keys.size();i++){
            Comparable a = this.keys.get(i);
            Comparable b = o.keys.get(i);
            if(a == null || b == null){
                throw new RuntimeException("Bad input data,index is " + i);
            }
            int ret = a.compareTo(b);
            if(ret!=0){
                return ret;
            }
        }
        return 0;
    }
}
