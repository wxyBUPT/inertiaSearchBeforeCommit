package com.alibaba.middleware.race;

/**
 * Created by xiyuanbupt on 7/10/16.
 */
public class RaceConf {

    //磁盘扇区大小
    private static final int DISKSIZE = 512;
    public static final int INDEXNODEMAXSIZE = 1200;
    /**
     * 内存中索引存储最多key 的数量
     */
    /**
     * 比赛环境使用
     */
    public static final int INMEMORYMAXINDEXSIZE = 240000;
    /**
     * 测试环境使用
     */
    //public static final int INMEMORYMAXINDEXSIZE = 2400;

    public static final boolean debug = true;
}
