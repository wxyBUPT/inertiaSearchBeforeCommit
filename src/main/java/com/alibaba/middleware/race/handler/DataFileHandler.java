package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.storage.*;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Collection;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public abstract class DataFileHandler{


    protected OriginalExtentManager originalExtentManager;
    abstract void handleLine(String line,DiskLoc diskLoc) throws IOException,OrderSystem.TypeException,InterruptedException;

    public void handle(Collection<String> files) throws InterruptedException,IOException,OrderSystem.TypeException{
        this.originalExtentManager= OriginalExtentManager.getInstance();
        for(String file:files) {
            handleFile(file);
        }
    }

    private void handleFile(String file){
        /**
         * 可以从一个file 中创建多个extent.并放入originalExtentManager 中
         */
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel channel = raf.getChannel();

            /**
             * 当前extent 的开始位置
             */
            Long currentExtentPosition = raf.getFilePointer();

            /**
             * 当前extent 的逻辑标号
             */

            int currentExtentNum = originalExtentManager.applyExtentNo();

            /**
             * 当前行数的位置
             */
            Long currentLinePosition = raf.getFilePointer();

            /**
             * 当前待处理的数据行
             */
            String line = raf.readLine();

            /**
             * 在一个extent 中已经处理的行数
             */
            Integer lineCount = 0;
            while (line!=null){
                if(lineCount>=100000){
                    originalExtentManager.putExtent(
                            new OrigionExtent(
                                    channel,
                                    currentExtentPosition,
                                    raf.getFilePointer()-currentExtentPosition,
                                    currentExtentNum)
                    );

                    /**
                     * 更新当前extent 的位置,同时更新当前extent 的逻辑标号
                     */
                    currentExtentPosition = raf.getFilePointer();
                    currentExtentNum = originalExtentManager.applyExtentNo();
                }
                DiskLoc diskLoc = new DiskLoc(currentExtentNum,
                        currentLinePosition.intValue()-currentExtentPosition.intValue(),
                        StoreType.NOTDEFINED,line.length());
                currentLinePosition = raf.getFilePointer();
                handleLine(line,diskLoc);
                line = raf.readLine();
            }

            /**
             * 要定位最后的extent
             */
            originalExtentManager.putExtent(
                    new OrigionExtent(
                            channel,
                            currentExtentPosition,
                            raf.getFilePointer()-currentExtentPosition,
                            currentExtentNum
                    )
            );
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }


    }
}
