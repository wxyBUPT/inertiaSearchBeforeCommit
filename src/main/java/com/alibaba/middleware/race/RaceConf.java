package com.alibaba.middleware.race;

import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.IndexLeafNode;
import com.alibaba.middleware.race.storage.StoreType;

/**
 * Created by xiyuanbupt on 7/10/16.
 *
 */
public class RaceConf {
    /**
     * 全量数据估计
     *
     * order 条目 470804597 也就是接近一亿
     * order 大小 102400M  也就是 100
     * good 条目 4708045 五百万
     * good 大小 4304M 也就是 4G
     * buyer 条目 9416090  九百万
     * buyer 大小 2354 也就是2G
     *
     * 磁盘一个扇区的大小一般是4K,在调整INDEXNODEMAXSIZE 之后运行main方法确认是否合适
     * 另外调整INDEXNODEMAXSIZE 之后需要调整 NINDEX count ,以获得最佳性能
     *
     * 一个 comparablekeys在缓存中内存大小大概是   200 byte 到 400 byte
     */

    //一个index 保存最大的索引量
    public static final int INDEXNODEMAXSIZE = 800;
    /**
     * 内存中索引存储最多key 的数量
     */
    /**
     * 比赛环境使用,即一个avl 节点中最大容量为  ......
     */
    public static final int INMEMORYMAXINDEXSIZE = 1600000;
    /**
     * 测试环境使用
     */
    //public static final int INMEMORYMAXINDEXSIZE = 2400;

    public static final boolean debug = true;

    /**
     * 一个Extent 的大小,以及一个文件中Extent 中的数目
     */
    public static final Long extentSize = (long)1024 * 1024 * 128;
    public static final Integer nExtentPerFile ;
    static {
        if(debug)nExtentPerFile=16;
        else nExtentPerFile=16;
    }

    /**
     * 在内存中LRU 保存最大的indexNode 数量
     */
    /**
     * good index 缓存数量,原始文件中,buyer 是 good 数量的 2倍
     */
    public static final Integer N_GOOD_INDEX_CACHE_COUNT = 10000;

    /**
     * buyer index 缓存的数量
     */
    public static final Integer N_BUYER_INDEX_CACHE_COUNT = 20000;
    /**
     * order index 缓存的数量
     */
    public static final Integer N_ORDER_INDEX_CACHE_COUNT = 20000;

    public static void main(String[] args){
        int TESTCOUNT = 1200;
        IndexLeafNode<ComparableKeysByBuyerCreateTimeOrderId> inIndexLeafNode =
                new IndexLeafNode<>();
        IndexLeafNode<ComparableKeysByOrderId> orderIds = new IndexLeafNode<>();
        IndexLeafNode<ComparableKeysByGoodOrderId> goodOrderIds = new
                IndexLeafNode<>();
        IndexLeafNode<ComparableKeysByBuyerId> byBuyerIds = new IndexLeafNode<>();
        for(int i = 0;i<TESTCOUNT;i++){
            if(!inIndexLeafNode.isFull()) {
                inIndexLeafNode.appendData(
                        new ComparableKeysByBuyerCreateTimeOrderId("ap-83a7-9c56d34045aa",
                                1L, 2L, new DiskLoc(0, 0, StoreType.GOODLINE, 0))
                );
                orderIds.appendData(new ComparableKeysByOrderId(
                        1L,new DiskLoc(0,0,StoreType.BUYERLINE,0)
                ));
                goodOrderIds.appendData(new ComparableKeysByGoodOrderId(
                        "dd-b00a-d67c9f59ce06",2L,new DiskLoc(0, 0, StoreType.GOODLINE, 0)
                ));
                byBuyerIds.appendData(new ComparableKeysByBuyerId(
                     "ap-83a7-9c56d34045aa",new DiskLoc(0,0,StoreType.GOODLINE,0)
                ));
            }
            else {
                break;
            }
        }
        byte[] bytes = SerializationUtils.serialize(inIndexLeafNode);
        System.out.println("一个 " + RaceConf.INDEXNODEMAXSIZE + "的最长节点长度是" + bytes.length/1024 + "KByte");
        bytes = SerializationUtils.serialize(orderIds);
        System.out.println("一个 " + RaceConf.INDEXNODEMAXSIZE + "的orderID长度是" + bytes.length/1024 + "KB");
        bytes = SerializationUtils.serialize(goodOrderIds);
        System.out.println("一个 " + RaceConf.INDEXNODEMAXSIZE + "的goodOrderID长度是" + bytes.length/1024 + "KB");
        bytes = SerializationUtils.serialize(byBuyerIds);
        System.out.println("一个 " + RaceConf.INDEXNODEMAXSIZE + "的buyerId长度是" + bytes.length/1024 + "KB");
    }
}
