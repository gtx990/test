package io.openmessaging;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    public final static int BOUND = 5000;                           //此次文件的消息个数
    //public final static int BOUND = 5000;
    public final static int BUFCAP = 50000;
    //public final static int BUFCAP = 24000;                                 //25000000
    public final static String PATH = "/alidata1/race2019/data/data_";
    //public final static String PATH = "./data_";
    public static List<Message> msgList = new ArrayList<>(BOUND);                     //存放消息的list
    public static AtomicInteger count = new AtomicInteger(0);             //实时计算当下消息的个数
    public static Semaphore getNewList = new Semaphore(1);                   //到达BOUND时，新到达的消息需要获取信号量
    public static Semaphore sendOver = new Semaphore(1);
    public static Semaphore getMeg = new Semaphore(0);
    public static AtomicInteger fileNum = new AtomicInteger(0);                                      //文件计数
    public static List<long[]> indexList = new ArrayList<>();           //存放每个文件时间戳边界的list
    public static Semaphore writeLimiter=new Semaphore(1);
    public static Semaphore threadLimiter=new Semaphore(20);
    public static Semaphore mutex=new Semaphore(1);
    @Override
    public synchronized void put(Message message) {
        try {
            threadLimiter.acquire();
            int tempCount = count.incrementAndGet();                        //每个消息加1
            if (tempCount < BOUND) {
                msgList.add(tempCount - 1, message);                            //按序号放入消息
                if (tempCount == BOUND / 2) {
                    try {
                        getNewList.acquire();                               //消除多出的1个信号量
                    } catch (Exception e1) {
                        System.out.println(e1);
                    }
                }
            } else if (tempCount == BOUND) {                               //到达1个文件的消息数量时
                msgList.add(tempCount - 1, message);
                List<Message> tempList = msgList;
                msgList = new ArrayList<>(BOUND);                                //新建空的list继续接收消息
                count.getAndAdd(-1 * BOUND);
                getNewList.release();                                       //可以继续向新list写消息了
                threadLimiter.acquire();
                new Thread(new Runnable() {                                 //新线程写文件
                    @Override
                    public void run() {
                        try {
                            Collections.sort(tempList, new Comparator<Message>() {
                                @Override
                                public int compare(Message o1, Message o2) {
                                    Long To1 = o1.getT();
                                    Long To2 = o2.getT();
                                    return To1.compareTo(To2);
                                }
                            });                                                              //排序
                            long[] tempIndex = new long[2];
                            tempIndex[0] = tempList.get(0).getT();
                            tempIndex[1] = tempList.get(BOUND - 1).getT();
                            mutex.acquire();
                            int fn = fileNum.getAndIncrement();
                            indexList.add(fn, tempIndex);                                        //构建索引
                            mutex.release();
                            writeFile(tempList, fn);                                             //Write File
                            threadLimiter.release();
                        }catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }).start();
            } else {
                try {
                    getNewList.acquire();
                    msgList.add(tempCount - BOUND - 1, message);               //超过BOUND-1000的消息获取信号量之后，tempCount减去（BOUND-1000）是在新list里的序号
                    getNewList.release();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            threadLimiter.release();
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        long pre = System.currentTimeMillis();
        if(sendOver.tryAcquire()){
            if(msgList.size() != 0) {
                Collections.sort(msgList, new Comparator<Message>() {
                    @Override
                    public int compare(Message o1, Message o2) {
                        Long To1 = o1.getT();
                        Long To2 = o2.getT();
                        return To1.compareTo(To2);
                    }
                });
                long[] tempIndex = new long[2];
                tempIndex[0] = msgList.get(0).getT();
                tempIndex[1] = msgList.get(msgList.size() - 1).getT();
                int fn = fileNum.get();
                indexList.add(fn, tempIndex);                                        //最后一次 构建索引
                writeFile(msgList, fn);
            }
            msgList = null;
            System.gc();
            /*
            for(long index[] : indexList){
                System.out.println("file_" + indexList.indexOf(index) + ": " + index[0] + " " + index[1]);
            }
            */
            getMeg.release(2000000000);
        }
        List<Integer> searchFileList = new ArrayList<>();
        try{
            getMeg.acquire();
            searchFileList = SearchFile.searchInstance.searchSuitableFiles(indexList, tMin, tMax);    //获取合适的文件列表
            /*
            System.out.println("aMin:" + aMin + " aMax:" + aMax + " tMin:" + tMin + " tMax:" + tMax);
            for(Integer num : searchFileList){
                System.out.print(num + " ");
            }
            */
        } catch (Exception e){
            e.printStackTrace();
        }
        CalculateMeg calculateMeg = new CalculateMeg(searchFileList);
        List<Message> res = calculateMeg.returnMsg(aMin, aMax, tMin, tMax);
        long aft = System.currentTimeMillis();
        //System.out.println("cost time : " + (aft - pre) + " aMin:" + aMin + " aMax:" + aMax + " tMin:" + tMin + " tMax:" + tMax);
        return res;
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        List<Integer> searchFileList = new ArrayList<>();
        try{
            getMeg.acquire();
            searchFileList = SearchFile.searchInstance.searchSuitableFiles(indexList, tMin, tMax);    //获取合适的文件列表
        } catch (Exception e){
            System.out.println(e);
        }
        CalculateMeg calculateMeg = new CalculateMeg(searchFileList);
        return calculateMeg.returnAvg(aMin, aMax, tMin, tMax);
    }

    public void writeFile(List<Message> tempList, int fn){
        try{

            FileOutputStream fos = new FileOutputStream(PATH + fn);
            BufferedOutputStream bos=new BufferedOutputStream(fos);
            ByteBuffer partBuffer = ByteBuffer.allocate(BUFCAP);       //消息缓冲区
            ListIterator<Message> it = tempList.listIterator();          //迭代消息list
            byte[] buf;
            while(it.hasNext()){
                Message mes = it.next();
                partBuffer.putLong(mes.getA());
                partBuffer.putLong(mes.getT());
                partBuffer.put(mes.getBody());
                buf=partBuffer.array();
                if(!partBuffer.hasRemaining()){
                    writeLimiter.acquire();
                    bos.write(buf);                       //消息缓冲区的数据写进文件
                    bos.flush();
                    writeLimiter.release();
                    partBuffer.clear();                                  //清空消息缓冲区

                }
            }
            if(partBuffer.position() != 0) {
                partBuffer.flip();
                byte[] bb = new byte[partBuffer.limit()];
                partBuffer.get(bb, 0, partBuffer.limit());
                writeLimiter.acquire();
                bos.write(bb);
                bos.flush();                                                 //把写缓冲区剩余的数据写进文件
                writeLimiter.release();
                partBuffer.clear();
            }
            bos.close();
        } catch (Exception e){
            System.out.println(e);
        }
    }

}
