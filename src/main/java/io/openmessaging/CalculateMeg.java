package io.openmessaging;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class CalculateMeg {
    private List<Integer> fileList;

    public CalculateMeg(List<Integer> fileList){
        this.fileList = fileList;
    }

    public List<Integer> getFileList() {
        return fileList;
    }

    public List<Message> returnMsg(long aMin, long aMax, long tMin, long tMax){           //读文件选message
        List<Message> source;
        List<Message> res = new ArrayList<>();
        ReadFile readFile = new ReadFile();
        for(int num : getFileList()) {
            source = readFile.readFiles(num);
            res = msgFilter(res, source, aMin, aMax, tMin, tMax);
        }
        Collections.sort(res, new Comparator<Message>() {
            @Override
            public int compare(Message o1, Message o2) {
                Long To1 = o1.getT();
                Long To2 = o2.getT();
                return To1.compareTo(To2);
            }
        });
        return res;
    }

    public long returnAvg(long aMin, long aMax, long tMin, long tMax){                   //读文件计算平均值
        List<Message> source;
        long[] res = {0, 0};                                                            //res[0]总大小，res[1]个数
        ReadFile readFile = new ReadFile();
        for(int num : getFileList()) {
            source = readFile.readFiles(num);
            res = avgFilter(res, source, aMin, aMax, tMin, tMax);
        }
        return res[1] == 0 ? 0 : res[0] / res[1];
    }

    public List<Message> msgFilter(List<Message> resList, List<Message> src, long aMin, long aMax, long tMin, long tMax){     //过滤合适的message
        int idFirst = binarySearchLowerBound(src, tMin);
        int idEnd = binarySearchUpperBound(src, tMax);
        for(int j = idFirst; j <= idEnd; j++){
            Message tempM = src.get(j);
            if(tempM.getA() >= aMin && tempM.getA() <= aMax){
                resList.add(new Message(tempM.getA(), tempM.getT(), tempM.getBody()));
            }
        }
        return resList;
    }

    public long[] avgFilter(long[] res, List<Message> src, long aMin, long aMax, long tMin, long tMax){          //过滤合适的message计算avg
        int idFirst = binarySearchLowerBound(src, tMin);
        int idEnd = binarySearchUpperBound(src, tMax);
        for(int j = idFirst; j <= idEnd; j++){
            Message tempM = src.get(j);
            if(tempM.getA() >= aMin && tempM.getA() <= aMax){
                res[0] += tempM.getA();
                res[1]++;
            }
        }
        return res;
    }

    public int binarySearchLowerBound(List<Message> list, long target){
        if(list == null){
            System.out.println("is null");
        }
        if (target <= list.get(0).getT()) {
            return 0;
        }
        int mid = binarySearch(list, target);
        while(list.get(mid-1).getT() == target){
            mid--;
        }
        return mid;
    }

    public int binarySearchUpperBound(List<Message> list, long target){
        if (target >= list.get(list.size()-1).getT()) {
            return list.size() - 1;
        }
        int mid = binarySearch(list, target);
        mid--;
        while(list.get(mid+1).getT() == target){
            mid++;
        }
        return mid;
    }

    public int binarySearch(List<Message> list, long target){
        int low = 0;
        int high = list.size() - 1;
        while(low <= high){
            int mid = (low + high) / 2;
            if(list.get(mid).getT() < target){
                low = mid + 1;
            }
            else if(list.get(mid).getT() > target){
                high = mid - 1;
            }
            else{
                return mid;
            }
        }
        return low;
    }
}
