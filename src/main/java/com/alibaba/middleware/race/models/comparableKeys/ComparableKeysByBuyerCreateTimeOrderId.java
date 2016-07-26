package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/18/16.
 */
public class ComparableKeysByBuyerCreateTimeOrderId implements Indexable,Comparable<ComparableKeysByBuyerCreateTimeOrderId>,Serializable{

    private String buyerId;
    private Long createTime;
    private Long orderId;
    private DiskLoc diskLoc;

    @Override
    public int compareTo(ComparableKeysByBuyerCreateTimeOrderId o) {
        if(this.orderId==null||this.createTime==null||this.buyerId==null
                ||o.orderId==null||o.createTime==null||o.buyerId==null){
            throw new RuntimeException("Bad keys, there is a bug maybe");
        }
        int ret = this.buyerId.compareTo(o.buyerId);
        if(ret!=0){
            return ret;
        }
        ret = this.createTime.compareTo(o.createTime);
        if(ret != 0){
            return ret;
        }
        ret = this.orderId.compareTo(o.orderId);
        return ret;
    }

    public ComparableKeysByBuyerCreateTimeOrderId(
            String buyerId,Long createTime,Long orderId,DiskLoc diskLoc
    ){
        this.buyerId = buyerId;
        this.createTime = createTime;
        this.orderId = orderId;
        this.diskLoc = diskLoc;
    }

    @Override
    public DiskLoc getDataDiskLoc() {
        return diskLoc;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(ComparableKeysByBuyerCreateTimeOrderId.class.getName()+" : ");
        sb.append("BuyerId: " + buyerId).append(", createTime: " + createTime + ",orderId: " + orderId);
        return sb.toString();
    }

    @Override
    public int hashCode(){
        /**
         * 因为是为了范围查询,一个buyer 应该路由到一个extent 中去
         * 即保证相同的buyer 落在一个桶里面
         */
        Integer res = buyerId.hashCode();
        return Math.abs(res);
    }

}
