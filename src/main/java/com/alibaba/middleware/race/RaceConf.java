package com.alibaba.middleware.race;

/**
 * Created by xiyuanbupt on 7/10/16.
 */
public class RaceConf {

    //磁盘扇区大小
    public static final int INDEXNODEMAXSIZE = 800;
    /**
     * 内存中索引存储最多key 的数量
     */
    /**
     * 比赛环境使用,即一个avl 节点中最大容量为  ......
     */
    public static final int INMEMORYMAXINDEXSIZE = 2400000;
    /**
     * 测试环境使用
     */
    //public static final int INMEMORYMAXINDEXSIZE = 2400;

    public static final boolean debug = true;

    /**
     * 一个Extent 的大小,以及一个文件中Extent 中的数目
     */
    public static final Long extentSize = (long)1024 * 1024 * 64;
    public static final Integer nExtentPerFile ;
    static {
        if(debug)nExtentPerFile=32;
        else nExtentPerFile=32;
    }

    /**
     * 在内存中LRU 保存最大的indexNode 数量
     */
    /**
     * good index 缓存数量,原始文件中,buyer 是 good 数量的 2倍
     */
    public static final Integer N_GOOD_INDEX_CACHE_COUNT = 3000;

    /**
     * buyer index 缓存的数量
     */
    public static final Integer N_BUYER_INDEX_CACHE_COUNT = 6000;
    /**
     * order index 缓存的数量
     */
    public static final Integer N_ORDER_INDEX_CACHE_COUNT = 10000;

}
