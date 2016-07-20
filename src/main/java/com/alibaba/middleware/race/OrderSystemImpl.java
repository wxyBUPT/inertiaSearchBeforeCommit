package com.alibaba.middleware.race;

import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.decoupling.BuildBuyerIdThread;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;
import com.alibaba.middleware.race.models.comparableKeys.*;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.FileInfoBean;
import com.alibaba.middleware.race.storage.FileManager;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/13/16.
 * 查询入口类
 */
public class OrderSystemImpl implements OrderSystem {
    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
    }

    private static final Logger LOG = Logger.getLogger(OrderSystemImpl.class.getName());
    static private String nameSpace = "tianchi";

    //根据三个表里面的主键查询
    final List<String> comparableKeysOrderingByOrderId;
    final List<String> comparableKeysOrderingByGood;
    final List<String> comparableKeysOrderingByBuyer;

    private FileManager fileManager;

    public OrderSystemImpl() {
        comparableKeysOrderingByBuyer = new ArrayList<>();
        comparableKeysOrderingByGood = new ArrayList<>();
        comparableKeysOrderingByOrderId = new ArrayList<>();

        comparableKeysOrderingByBuyer.add("buyerid");
        comparableKeysOrderingByGood.add("goodid");
        comparableKeysOrderingByOrderId.add("orderid");
    }

    /**
     * 输入文件,创建索引,每三个文件一个输入线程,七个创建索引线程
     *
     * @param orderFiles   订单文件列表
     * @param buyerFiles   买家文件列表
     * @param goodFiles    商品文件列表
     * @param storeFolders
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void construct(final Collection<String> orderFiles, final Collection<String> buyerFiles, final Collection<String> goodFiles, final Collection<String> storeFolders) throws IOException, InterruptedException {
        /**
         * 三个文件一个线程
         */

        final FileManager fileManager = FileManager.getInstance(storeFolders, nameSpace);
        Collection<Collection<String>> splitedOrderFiles = spliter(orderFiles, 3);
        Collection<Collection<String>> splitedBuyerFiles = spliter(buyerFiles, 3);
        Collection<Collection<String>> splitedGoodFiles = spliter(goodFiles, 3);
        int nThread = splitedBuyerFiles.size() + splitedGoodFiles.size() + splitedOrderFiles.size();
        LOG.info("There will be " + nThread + "fileWriter threads");

        /**
         * set countdownlatch,wait all file read finish
         */
        final CountDownLatch doneSignal = new CountDownLatch(nThread);

        final AtomicInteger nOrderRemain = new AtomicInteger(0);
        final AtomicInteger nGoodRemain = new AtomicInteger(0);
        final AtomicInteger nBuyerRemain = new AtomicInteger(0);

        CountDownLatch indexDoneSignal = new CountDownLatch(7);

        /**
         * nThread 个将原始数据写入磁盘的线程
         */
        for (final Collection<String> files : splitedOrderFiles) {
            nOrderRemain.incrementAndGet();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new OrderFileHandler().handle(files, fileManager);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    nOrderRemain.decrementAndGet();
                    doneSignal.countDown();
                }
            }).start();
            LOG.info("new orderFiles writer have started");
        }

        for (final Collection<String> files : splitedBuyerFiles) {
            nBuyerRemain.incrementAndGet();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new BuyerFileHandler().handle(files, fileManager);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    nBuyerRemain.decrementAndGet();
                    doneSignal.countDown();
                }
            }).start();
            LOG.info("new buyerFiles writer have started");
        }

        for (final Collection<String> files : splitedGoodFiles) {
            nGoodRemain.incrementAndGet();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new GoodFileHandler().handle(files, fileManager);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    nGoodRemain.decrementAndGet();
                    doneSignal.countDown();
                }
            }).start();
            LOG.info("new goodFiles writer have started");
        }
        /**
         * 七个创建索引线程
         */

        doneSignal.await();
        LOG.info("finish copy file");
        indexDoneSignal.await();
        LOG.info("finish create all");
    }

    /**
     * 将一个collections 分成多个collections,每个collection 中至少有n 个元素
     *
     * @param files
     * @param n
     * @return
     */
    private Collection<Collection<String>> spliter(Collection<String> files, int n) {
        Collection<Collection<String>> res = new ArrayList<>();
        Collection<String> item = new ArrayList<>();
        int count = 0;
        for (String file : files) {
            item.add(file);
            count++;
            if (count == n) {
                res.add(item);
                item = new ArrayList<>();
                count = 0;
            }
        }
        if (item.size() != 0) res.add(item);
        return res;
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        return null;
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        return null;
    }

    private Collection<String> getFolderFiles(Collection<String> files, String storeFolder) {
        Collection<String> floderFiles = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith(storeFolder)) {
                floderFiles.add(file);
            }
        }
        return floderFiles;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {

        // init order system
        List<String> orderFiles = new ArrayList<>();
        List<String> buyerFiles = new ArrayList<>();
        ;
        List<String> goodFiles = new ArrayList<>();
        List<String> storeFolders = new ArrayList<>();

        orderFiles.add("order_records.txt");
        buyerFiles.add("buyer_records.txt");
        goodFiles.add("good_records.txt");
        storeFolders.add("./dir0");
        storeFolders.add("./dir1");
        storeFolders.add("./dir2");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
    }
}


abstract class DataFileHandler{

    protected FileManager fileManager;
    protected MappedByteBuffer currentFile;
    protected int currentFileNum;
    protected int currentOff;

    abstract void handleRow(Row row) throws IOException,OrderSystem.TypeException,InterruptedException;

    void handle(Collection<String> files,FileManager fileManager) throws InterruptedException,IOException,OrderSystem.TypeException{
        this.fileManager = fileManager;
        FileInfoBean fib = fileManager.createStoreFile();
        currentFile = fib.getBuffer();
        currentFileNum = fib.getfileN();
        currentOff = 0;
        for(String file:files) {
            BufferedReader bfr = createReader(file);
            try {
                String line = bfr.readLine();
                while (line != null) {
                    Row kvMap = createKVMapFromLine(line);
                    handleRow(kvMap);
                    line = bfr.readLine();
                }
            } finally {
                bfr.close();
            }
        }
    }

    private BufferedReader createReader(String file) throws FileNotFoundException{
        return new BufferedReader((new FileReader(file)));
    }

    private Row createKVMapFromLine(String line){
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            RowKV kv = new RowKV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }

    /**
     * Judge if current file can store %sbyte%(size) ,if can't create new file ;
     * @param size
     * @throws IOException
     */
    protected void updataFile(int size) throws IOException{
        if(currentFile.remaining()<size+4){
            FileInfoBean fib = fileManager.createStoreFile();
            currentFile = fib.getBuffer();
            currentFileNum = fib.getfileN();
            currentOff = 0;
        }
    }
}

class GoodFileHandler extends DataFileHandler{

    @Override
    void handleRow(Row row) throws IOException,InterruptedException{
        byte[] byteRow = SerializationUtils.serialize(row);
        int size = byteRow.length;
        /**
         * If file can't save more, create new File;
         */
        updataFile(size);
        currentFile.put(byteRow);
        /**
         * Create new diskLoc
         */
        DiskLoc diskLoc = new DiskLoc(this.currentFileNum,this.currentOff, StoreType.ROWDATA,size);
        currentOff += size;
        /**
         * Put index info to queue
         */
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(row.get("goodid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
        ComparableKeysBySalerIdGoodId salerGoodKeys = new ComparableKeysBySalerIdGoodId(
                row.get("salerid").valueAsString(),row.get("goodid").valueAsString(),diskLoc
        );
        DiskLocQueues.comparableKeysBySalerIdGoodId.put(salerGoodKeys);
    }
}

class BuyerFileHandler extends DataFileHandler{
    @Override
    void handleRow(Row row) throws IOException,InterruptedException {
        byte[] byteRow = SerializationUtils.serialize(row);
        int size = byteRow.length;
        /**
         * If file can't save more, create new File;
         */
        updataFile(size);
        currentFile.put(byteRow);
        /**
         * Create new diskLoc
         */
        DiskLoc diskLoc = new DiskLoc(this.currentFileNum,this.currentOff, StoreType.ROWDATA,size);
        currentOff += size;
        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId buyerIdKeys = new ComparableKeysByBuyerId(row.get("buyerid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(buyerIdKeys);
    }
}

class OrderFileHandler extends DataFileHandler{
    @Override
    void handleRow(Row row) throws IOException,OrderSystem.TypeException,InterruptedException{
        byte[] byteRow = SerializationUtils.serialize(row);
        int size = byteRow.length;
        /**
         * If file can't save more, create new File;
         */
        updataFile(size);
        currentFile.put(byteRow);
        /**
         * Create new diskLoc
         */
        DiskLoc diskLoc = new DiskLoc(this.currentFileNum,this.currentOff, StoreType.ROWDATA,size);
        currentOff += size;
        /**
         * Put index info to queue
         */
        ComparableKeysByOrderId orderIdKeys = new ComparableKeysByOrderId(row.get("orderid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByOrderId.put(orderIdKeys);
        ComparableKeysByBuyerCreateTimeOrderId buyerCreateTimeOrderId = new ComparableKeysByBuyerCreateTimeOrderId(
                row.get("buyerid").valueAsString(), row.get("createtime").valueAsLong(), row.get("orderid").valueAsLong(),diskLoc
        );
        DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId.put(buyerCreateTimeOrderId);

        ComparableKeysBySalerGoodOrderId salerGoodOrderKeys = new ComparableKeysBySalerGoodOrderId(
                row.get("goodid").valueAsString(),row.get("orderid").valueAsLong(),diskLoc
        );
        DiskLocQueues.comparableKeysBySalerGoodOrderId.put(salerGoodOrderKeys);

        ComparableKeysByGoodOrderId goodOrderKeys = new ComparableKeysByGoodOrderId(
                row.get("goodid").valueAsString(),row.get("orderid").valueAsLong(),diskLoc
        );
        DiskLocQueues.comparableKeysByGoodOrderId.put(goodOrderKeys);
    }
}
