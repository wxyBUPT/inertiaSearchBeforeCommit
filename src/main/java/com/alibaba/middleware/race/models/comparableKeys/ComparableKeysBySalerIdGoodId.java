package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysBySalerIdGoodId implements Comparable<ComparableKeysBySalerIdGoodId>,Serializable{

    private DiskLoc diskLoc;
    private String salerId;
    private String goodId;

    public ComparableKeysBySalerIdGoodId(String salerId,String goodId,DiskLoc diskLoc){
        this.diskLoc = diskLoc;
        this.salerId = salerId;
        this.goodId = goodId;
    }

    @Override
    public int compareTo(ComparableKeysBySalerIdGoodId o) {
        if(this.salerId==null||this.goodId==null||o.salerId == null||o.goodId==null){
            throw new RuntimeException("Bad keys");
        }
        /**
         * Compare salerId
         */
        if(!this.salerId.equals(o.salerId)){
            return this.salerId.compareTo(o.salerId);
        }
        /**
         * Compare goodId
         */
        if(this.goodId.equals(o.goodId)){
            throw new RuntimeException("Got same salerId + goodId,there may be some bug");
        }
        return this.goodId.compareTo(o.goodId);
    }

    public DiskLoc getDiskLoc(){
        return diskLoc;
    }
}
