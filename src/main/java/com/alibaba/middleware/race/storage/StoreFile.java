package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/23/16.
 * 抽象出一层管理存储文件的file
 * 所有文件创建工作,extent 创建工作都放在这一层
 */
public class StoreFile {
    /**
     * 文件的名字
     */
    String fileDir;

    /**
     * 拥有的所有Extents
     */
    Vector<Extent> myExtents;

    /**
     * 所属的逻辑文件的逻辑标号
     */
    int fileNo;

    /**
     * myFile
     */
    private RandomAccessFile myFile ;
    private FileChannel fileChannel;

    /**
     * 只能在构造阶段初始化的iterator,写入阶段一个iterator 只能消费一次
     */
    private Iterator<Extent> newExtentIterator;

    /**
     *
     * @param fileDir 策略层指定的fileName, 策略层已经计算好存储在哪块磁盘,使用哪个fileNo
     * @param fileNo
     */
    public StoreFile(String fileDir,int fileNo){
        this.fileDir = fileDir;
        /**
         * 每个文件里面有 nExtentPerFile 个Extent
         */
        myExtents = new Vector<>(RaceConf.nExtentPerFile);
        this.fileNo = fileNo;
        /**
         * 创建文件,并创建extent
         */
        try {
            myFile = new RandomAccessFile(fileDir, "rw");
            fileChannel = myFile.getChannel();
            /**
             * 第一个Extent 所属的逻辑标号
             */
            int startExtentNum = fileNo * RaceConf.nExtentPerFile;
            for(int i = 0;i<RaceConf.nExtentPerFile;i++){
                myExtents.add(new Extent(fileChannel,i*RaceConf.extentSize,RaceConf.extentSize,i+startExtentNum));
            }
            newExtentIterator = myExtents.iterator();

        }catch (FileNotFoundException e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * 在constrouct 最后阶段调用,即限制所有文件操作为只读
     */
    public synchronized void finshConstruct(){
        for(Extent extent:myExtents){
            extent.finishConstruct();
        }
        try {
            myFile = new RandomAccessFile(fileDir, "r");
            fileChannel = myFile.getChannel();
            for(Extent extent:myExtents){
                extent.makeReadOnly(fileChannel);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * 应当小心使用,调用了getNewExtent 即视为之前的extent 都被消费光
     * @return
     */
    public synchronized Extent getNewExtent(){
        if(newExtentIterator.hasNext()){
            return newExtentIterator.next();
        }
        return null;
    }

    public synchronized boolean hasNewExtent(){
        return newExtentIterator.hasNext();
    }

    /**
     * 测试函数
     * @param args
     */
    public static void main(String[] args){
        String fileDir = "/Users/xiyuanbupt/IdeaProjects/order-system/testFileDir/0.0";
        StoreFile storeFile = new StoreFile(fileDir,0);
        while(storeFile.hasNewExtent()){
            Extent extent = storeFile.getNewExtent();
            String foo = "王熙元";
            String bar = "你在哪";
            try {
                byte[] bytes = foo.getBytes("UTF-8");
                System.out.println(extent.putBytes(bytes));
                bytes = bar.getBytes("UTF-8");
                System.out.println(extent.putBytes(bytes));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        storeFile.finshConstruct();
        for(Extent extent:storeFile.myExtents){
            System.out.println(new String(extent.getBytesFromOfsAndSize(0,9)));
            System.out.println(new String(extent.getBytesFromOfsAndSize(9,9)));
        }
    }
}
