package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysByGoodId implements Indexable,Comparable<ComparableKeysByGoodId>,Serializable{

    private String goodId;
    private DiskLoc dataDiskLoc;

    public ComparableKeysByGoodId(String goodId,DiskLoc dataDiskLoc){
        this.goodId = goodId;
        this.dataDiskLoc = dataDiskLoc;
    }

    @Override
    public int compareTo(ComparableKeysByGoodId o) {
        return this.goodId.compareTo(o.goodId);
    }

    @Override
    public String toString(){
        return "ComparableKeysByGoodId: goodId: " + goodId;
    }

    @Override
    public DiskLoc getDataDiskLoc(){
        return dataDiskLoc;
    }

}
