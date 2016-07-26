package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysByGoodOrderId implements Indexable,Comparable<ComparableKeysByGoodOrderId>,Serializable{

    private String goodId;
    private Long orderId;
    private DiskLoc diskLoc;

    public ComparableKeysByGoodOrderId(String goodId,Long orderId,DiskLoc diskLoc){
        this.goodId = goodId;
        this.orderId = orderId;
        this.diskLoc = diskLoc;
    }

    public ComparableKeysByGoodOrderId(String goodId,Long orderId){
        this.goodId = goodId;
        this.orderId = orderId;
        this.diskLoc = null;
    }

    @Override
    public int compareTo(ComparableKeysByGoodOrderId o) {
        if(this.goodId==null||this.orderId==null||o.goodId==null||o.orderId==null){
            throw new RuntimeException("Bad keys, there is a bug maybe");
        }

        int ret = this.goodId.compareTo(o.goodId);
        if(ret != 0){
            return ret;
        }
        ret = this.orderId.compareTo(o.orderId);
        return ret;
    }

    @Override
    public String toString(){
        return "ComparableKeysByGoodOrderId: goodId: " + goodId + "orderId " + orderId;
    }

    @Override
    public DiskLoc getDataDiskLoc() {
        return diskLoc;
    }

    @Override
    public int hashCode(){
        Integer res = goodId.hashCode() + orderId.hashCode();
        return Math.abs(res);
    }
}
