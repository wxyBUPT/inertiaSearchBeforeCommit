package com.alibaba.middleware.race;

import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.decoupling.*;
import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;
import com.alibaba.middleware.race.models.comparableKeys.*;
import com.alibaba.middleware.race.storage.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    final AtomicLong queryOrderBySalerCount = new AtomicLong(0);
    final AtomicLong queryOrderByBuyerCount = new AtomicLong(0);
    final AtomicLong queryOrderCount = new AtomicLong(0);
    final AtomicLong queryOrderByGoodCount = new AtomicLong(0);

    //根据三个表里面的主键查询


    private FileManager fileManager;
    private IndexNameSpace indexNameSpace;

    public OrderSystemImpl() {
        /**
         * 时间
         */
        if(RaceConf.debug) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(5000);
                        } catch (Exception e) {
                        }
                        logStatus();
                    }
                }
            }).start();
        }
    }

    private void logStatus(){
        StringBuilder sb = new StringBuilder();
        sb.append("queryOrderBySalerCount is : " ).append(queryOrderBySalerCount);
        sb.append(",  queryOrderByBuyerCount is : " ).append(queryOrderByBuyerCount);
        sb.append(",  queryOrderCount is : "  ).append(queryOrderCount);
        sb.append(",  queryOrderByGoodCount is : ").append(queryOrderByGoodCount);
        LOG.info(sb.toString());
        LOG.info(DiskLocQueues.getInfo());

        sb.setLength(0);
                          /* Total number of processors or cores available to the JVM */
        sb.append("Available processors (cores): " +
                Runtime.getRuntime().availableProcessors());

  /* Total amount of free memory available to the JVM */
        sb.append(",  Free memory (Mbytes): " +
                Runtime.getRuntime().freeMemory()/1024L/1024L);

  /* This will return Long.MAX_VALUE if there is no preset limit */
        long maxMemory = Runtime.getRuntime().maxMemory();
  /* Maximum amount of memory the JVM will attempt to use */
        sb.append(",  Maximum memory (Mbytes): " +
                (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory/1024L/1024L));

  /* Total memory currently in use by the JVM */
        sb.append(",  Total memory (Mbytes): " +
                Runtime.getRuntime().totalMemory()/1024L/1024L);
        LOG.info(sb.toString());
        /**
         * Get LRU cache status
         */
        LOG.info(indexNameSpace.getInfo());
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
         * IndexExtentManager is a singleton , init it use paramter
         */
        IndexExtentManager.getInstance();
        FileManager.getInstance(storeFolders,nameSpace);
        indexNameSpace = IndexNameSpace.getInstance();
        /**
         * set countdownlatch,wait all file read finish
         */
        final CountDownLatch doneSignal = new CountDownLatch(nThread);

        final AtomicInteger nOrderRemain = new AtomicInteger(0);
        final AtomicInteger nGoodRemain = new AtomicInteger(0);
        final AtomicInteger nBuyerRemain = new AtomicInteger(0);

        final CountDownLatch indexDoneSignal = new CountDownLatch(5);



        /**
         * nThread 个将原始数据写入磁盘的线程
         */
        for (final Collection<String> files : splitedOrderFiles) {
            nOrderRemain.incrementAndGet();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new OrderFileHandler().handle(files);
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
                        new BuyerFileHandler().handle(files );
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
                        new GoodFileHandler().handle(files);
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

        new Thread(new BuildBuyerIdThread(nBuyerRemain,indexDoneSignal)).start();
        new Thread(new BuyerTimeOrderPartionBuildThread(nOrderRemain,indexDoneSignal)).start();
        new Thread(new BuildGoodIdThread(nGoodRemain,indexDoneSignal)).start();
        new Thread(new GoodOrderPartionBuildThread(nOrderRemain,indexDoneSignal)).start();
        new Thread(new OrderIdPartionBuildThread(nOrderRemain,indexDoneSignal)).start();
        /**
         * For debug
         */
        //new Thread(new Runnable() {
        //    @Override
        //    public void run() {
        //        while(true){
        //            try{
        //                Thread.sleep(3000);
        //            }catch (Exception e){

        //            }
        //            System.out.println(indexDoneSignal.getCount());
        //        }
        //    }
        //}).start();


        doneSignal.await();
        LOG.info("finish copy file");
        indexDoneSignal.await();
        LOG.info("finish create all");
        fileManager.finishConstruct();

    }

    private static class ResultImpl implements Result{
        private long orderid;
        private Row kvMap;

        private ResultImpl(long orderid,Row kv){
            this.orderid = orderid;
            this.kvMap = kv;
        }

        static private ResultImpl createResultRow(Row orderData, Row buyerData,
                                                  Row goodData, Set<String> queryingKeys) {
            if (orderData == null || buyerData == null || goodData == null) {
                throw new RuntimeException("Bad data!");
            }
            Row allkv = new Row();
            long orderid;
            try {
                orderid = orderData.get("orderid").valueAsLong();
            } catch (TypeException e) {
                throw new RuntimeException("Bad data!");
            }

            for (RowKV kv : orderData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            for (RowKV kv : buyerData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            for (RowKV kv : goodData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            return new ResultImpl(orderid, allkv);
        }

        public KeyValue get(String key) {
            return this.kvMap.get(key);
        }

        public KeyValue[] getAll() {
            return kvMap.values().toArray(new KeyValue[0]);
        }

        public long orderId() {
            return orderid;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("orderid: " + orderid + " {");
            if (kvMap != null && !kvMap.isEmpty()) {
                for (RowKV kv : kvMap.values()) {
                    sb.append(kv.toString());
                    sb.append(",\n");
                }
            }
            sb.append('}');
            return sb.toString();
        }
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
        queryOrderCount.incrementAndGet();
        Row orderData = indexNameSpace.queryOrderDataByOrderId(orderId);
        if(orderData == null){
            return null;
        }
        return createResultFromOrderData(orderData,createQueryKeys(keys));
    }

    private ResultImpl createResultFromOrderData(Row orderData,Collection<String> keys){
        String buyerId = orderData.get("buyerid").valueAsString();
        Row buyerData = indexNameSpace.queryBuyerDataByBuyerId(buyerId);
        String goodid = orderData.get("goodid").valueAsString();
        Row goodData = indexNameSpace.queryGoodDataByGoodId(goodid);
        return ResultImpl.createResultRow(orderData,buyerData,goodData,createQueryKeys(keys));
    }

    private ResultImpl createResultFromOrderGoodData(Row orderData,Row goodData,Collection<String> keys){
        String buyerId = orderData.get("buyerid").valueAsString();
        Row buyerData = indexNameSpace.queryBuyerDataByBuyerId(buyerId);
        return ResultImpl.createResultRow(orderData,buyerData,goodData,createQueryKeys(keys));
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        queryOrderByBuyerCount.incrementAndGet();
        final Deque<Row> orderDatas = indexNameSpace.queryOrderDataByBuyerCreateTime(startTime,endTime,buyerid);

        return new Iterator<Result>() {
            @Override
            public boolean hasNext() {
                return orderDatas != null && orderDatas.size()>0;
            }

            @Override
            public Result next() {
                if(!hasNext()){
                    return null;
                }
                Row orderData = orderDatas.pollLast();
                return createResultFromOrderData(orderData,null);
            }

            @Override
            public void remove() {

            }
        };
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, final Collection<String> keys) {
        queryOrderBySalerCount.incrementAndGet();
        final Row goodData = indexNameSpace.queryGoodDataByGoodId(goodid);
        final Queue<Row> orderDatas;
        if(goodData==null){
            orderDatas = null;
        }else {
            String querySalerId = goodData.get("salerid").valueAsString();
            if(salerid.compareTo(querySalerId)!=0){
                orderDatas = null;
            }else {
                orderDatas = indexNameSpace.queryOrderDataByGoodid(goodid);
            }
        }
        return new Iterator<Result>() {
            @Override
            public boolean hasNext() {
                return orderDatas != null && orderDatas.size()>0;
            }

            @Override
            public Result next() {
                if(!hasNext()){
                    return next();
                }
                Row orderData = orderDatas.poll();
                return createResultFromOrderGoodData(orderData,goodData,createQueryKeys(keys));
            }

            @Override
            public void remove() {

            }
        };
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        queryOrderByGoodCount.incrementAndGet();
        final Queue<Row> orderDatas = indexNameSpace.queryOrderDataByGoodid(goodid);
        List<ResultImpl> allData = new ArrayList<>(orderDatas.size());
        Row orderData = orderDatas.poll();
        HashSet<String> queryingKeys = new HashSet<>();
        queryingKeys.add(key);
        while(orderData!=null){
            allData.add(createResultFromOrderData(orderData,queryingKeys));
            orderData = orderDatas.poll();
        }

        //accumulate as Long
        try{
            boolean hasValidData = false;
            long sum = 0;
            for(ResultImpl r: allData){
                KeyValue kv = r.get(key);
                if(kv!=null){
                    sum +=kv.valueAsLong();
                    hasValidData = true;
                }
            }
            if(hasValidData){
                return new RowKV(key,Long.toString(sum));
            }
        }catch (TypeException e){

        }

        //accumulate as double
        try{
            boolean hasValidData = false;
            double sum = 0;
            for(ResultImpl r:allData){
                KeyValue kv = r.get(key);
                if(kv != null){
                    sum += kv.valueAsDouble();
                    hasValidData = true;
                }
            }
            if(hasValidData){
                return new RowKV(key,Double.toString(sum));
            }
        }
        catch (TypeException e){

        }
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

    private HashSet<String> createQueryKeys(Collection<String> keys) {
        if (keys == null) {
            return null;
        }
        return new HashSet<>(keys);
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
        orderFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/order.0.0");
        orderFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/order.1.1");
        orderFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/order.2.2");
        orderFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/order.0.3");

        buyerFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/buyer.0.0");
        buyerFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/buyer.1.1");

        goodFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/good.0.0");
        goodFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/good.1.1");
        goodFiles.add("/Users/xiyuanbupt/Downloads/prerun_data/good.2.2");
        storeFolders.add("./dir0/");
        storeFolders.add("./dir1/");
        storeFolders.add("./dir2/");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
        try{
            Thread.sleep(2);
        }catch (Exception e){

        }

        // 用例
        if(RaceConf.debug) {
            System.out.println("这些事测试用例");
            long orderid = 2982388;
            System.out.println("\n查询订单号为" + orderid + "的订单");
            System.out.println(os.queryOrder(orderid, null));

            System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
            System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

            System.out.println("\n查询订单号为" + orderid
                    + "的订单的contactphone, buyerid, foo, done, price字段");
            List<String> queryingKeys = new ArrayList<String>();
            queryingKeys.add("contactphone");
            queryingKeys.add("buyerid");
            queryingKeys.add("foo");
            queryingKeys.add("done");
            queryingKeys.add("price");
            Result result = os.queryOrder(orderid, queryingKeys);
            System.out.println(result);
            System.out.println("\n查询订单号不存在的订单");
            result = os.queryOrder(1111, queryingKeys);
            if (result == null) {
                System.out.println(1111 + " order not exist");
            }

            Iterator<Result> it;
            String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
            long startTime = 1471025622;
            long endTime = 1471219509;
            System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
            it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
            while (it.hasNext()) {
                System.out.println(it.next());
            }


            String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
            String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
            System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
            it = os.queryOrdersBySaler(salerid, goodid, queryingKeys);
            while (it.hasNext()) {
                System.out.println(it.next());
            }

            goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
            String attr = "app_order_33_0";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            System.out.println(os.sumOrdersByGood(goodid, attr));

            attr = "done";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            KeyValue sum = os.sumOrdersByGood(goodid, attr);
            if (sum == null) {
                System.out.println("由于该字段是布尔类型，返回值是null");
            }

            attr = "foo";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            sum = os.sumOrdersByGood(goodid, attr);
            if (sum == null) {
                System.out.println("由于该字段不存在，返回值是null");
            }
        }
    }
}


abstract class DataFileHandler{


    protected StoreExtentManager storeExtentManager;
    abstract void handleLine(String line) throws IOException,OrderSystem.TypeException,InterruptedException;

    void handle(Collection<String> files) throws InterruptedException,IOException,OrderSystem.TypeException{
        this.storeExtentManager = StoreExtentManager.getInstance();
        for(String file:files) {
            BufferedReader bfr = createReader(file);
            try {
                String line = bfr.readLine();
                while (line != null) {
                    handleLine(line);
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
}

class GoodFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line) throws IOException, OrderSystem.TypeException, InterruptedException {
        byte[] bytes = line.getBytes("UTF-8");
        DiskLoc diskLoc = storeExtentManager.putBytes(bytes);
        diskLoc.setStoreType(StoreType.GOODLINE);
        /**
         * Find goodid and salerid
         */

        String[] kvs = line.split("\t");
        String goodid = null;
        for(String kv: kvs){
            int p = kv.indexOf(':');
            String key = kv.substring(0, p);
            String value = kv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            if(key.equals("goodid")){
                goodid = value;
                break;
            }
        }
        if(goodid==null ){
            throw new RuntimeException("Bad data! goodid " + goodid  );
        }
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(goodid,diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
    }

    void handleRow(Row row) throws IOException,InterruptedException{
        byte[] byteRow = SerializationUtils.serialize(row);
        DiskLoc diskLoc = storeExtentManager.putBytes(byteRow);
        diskLoc.setStoreType(StoreType.ROWDATA);
        /**
         * Put index info to queue
         */
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(row.get("goodid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
    }
}

class BuyerFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line) throws IOException, OrderSystem.TypeException, InterruptedException {
        byte[] bytes = line.getBytes("UTF-8");
        DiskLoc diskLoc = storeExtentManager.putBytes(bytes);
        diskLoc.setStoreType(StoreType.BUYERLINE);
        String[] kvs = line.split("\t");

        /**
         * Find buyerid
         */
        String buyerid = null;
        for(String kv: kvs){
            int p = kv.indexOf(':');
            String key = kv.substring(0, p);
            String value = kv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            if(key.compareTo("buyerid")==0){
                buyerid = value;
                break;
            }
        }
        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerid,diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(key);
    }

    void handleRow(Row row) throws IOException,InterruptedException {
        byte[] byteRow = SerializationUtils.serialize(row);
        DiskLoc diskLoc = storeExtentManager.putBytes(byteRow);
        diskLoc.setStoreType(StoreType.ROWDATA);
        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId buyerIdKeys = new ComparableKeysByBuyerId(row.get("buyerid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(buyerIdKeys);
    }
}

class OrderFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line) throws IOException, OrderSystem.TypeException, InterruptedException {
        byte[] bytes = line.getBytes("UTF-8");
        DiskLoc diskLoc = storeExtentManager.putBytes(bytes);
        diskLoc.setStoreType(StoreType.ORDERLINE);
        String[] kvs = line.split("\t");
        /**
         * Find goodid and salerid
         */
        Long orderid= null;
        String buyerid = null;
        boolean shouldBreak = false;
        Long createtime = null;
        String goodid = null;

        for(String kv:kvs){
            int p = kv.indexOf(':');
            String key = kv.substring(0, p);
            String value = kv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            switch (key){
                case "orderid":
                    orderid = Long.parseLong(value);
                    if(buyerid!=null&&createtime!=null&&goodid!=null){
                        shouldBreak = true;
                    }
                    break;
                case "buyerid":
                    buyerid = value;
                    if(orderid!=null &&createtime!=null&&goodid!=null){
                        shouldBreak = true;
                    }
                    break;
                case "createtime":
                    createtime = Long.parseLong(value);
                    if(orderid!=null && buyerid!=null&&goodid!=null){
                        shouldBreak = true;
                    }
                    break;
                case "goodid":
                    goodid = value;
                    if(orderid!=null && buyerid!=null&&createtime!=null){
                        shouldBreak = true;
                    }
                    break;
                default:
                    break;
            }
            if(shouldBreak){
                break;
            }
        }
        ComparableKeysByOrderId orderIdKeys = new ComparableKeysByOrderId(orderid,diskLoc);
        DiskLocQueues.comparableKeysByOrderId.put(orderIdKeys);

        ComparableKeysByBuyerCreateTimeOrderId buyerCreateTimeOrderId = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid, createtime, orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId.put(buyerCreateTimeOrderId);

        ComparableKeysByGoodOrderId goodOrderKeys = new ComparableKeysByGoodOrderId(
                goodid,orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByGoodOrderId.put(goodOrderKeys);
    }

    void handleRow(Row row) throws IOException,OrderSystem.TypeException,InterruptedException{
        byte[] byteRow = SerializationUtils.serialize(row);
        DiskLoc diskLoc = storeExtentManager.putBytes(byteRow);
        diskLoc.setStoreType(StoreType.ROWDATA);

        /**
         * Put index info to queue
         */
        ComparableKeysByOrderId orderIdKeys = new ComparableKeysByOrderId(row.get("orderid").valueAsString(),diskLoc);
        DiskLocQueues.comparableKeysByOrderId.put(orderIdKeys);

        ComparableKeysByBuyerCreateTimeOrderId buyerCreateTimeOrderId = new ComparableKeysByBuyerCreateTimeOrderId(
                row.get("buyerid").valueAsString(), row.get("createtime").valueAsLong(), row.get("orderid").valueAsLong(),diskLoc
        );
        DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId.put(buyerCreateTimeOrderId);

        ComparableKeysByGoodOrderId goodOrderKeys = new ComparableKeysByGoodOrderId(
                row.get("goodid").valueAsString(),row.get("orderid").valueAsLong(),diskLoc
        );
        DiskLocQueues.comparableKeysByGoodOrderId.put(goodOrderKeys);
    }
}
