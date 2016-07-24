package com.alibaba.middleware.race;

/**
 * Created by xiyuanbupt on 7/10/16.
 */
public class RaceConf {

    //磁盘扇区大小
    public static final int INDEXNODEMAXSIZE = 1200;
    /**
     * 内存中索引存储最多key 的数量
     */
    /**
     * 比赛环境使用
     */
    public static final int INMEMORYMAXINDEXSIZE = 480000;
    /**
     * 测试环境使用
     */
    //public static final int INMEMORYMAXINDEXSIZE = 2400;

    public static final boolean debug = true;

    /**
     * 一个Extent 的大小,以及一个文件中Extent 中的数目
     */
    public static final Long extentSize = (long)1024 * 1024 * 64;
    public static final Integer nExtentPerFile = 32;
}
