package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

public class SearchFile {

    public static SearchFile searchInstance = new SearchFile();

    public List<Integer> searchSuitableFiles(List<long[]> fileList, long tMin, long tMax){
        List<Integer> list = new ArrayList<>();
        for (long[] bound: fileList) {
            if(bound[0] >= tMin && bound[0] <= tMax || bound[1] >= tMin && bound[1] <= tMax || bound[0] <= tMin && bound[1] >= tMax ){
                list.add(fileList.indexOf(bound));
            }
        }
        return list;
    }

    /*******************************************************************************
    public int SearchLowerIndex(List<long[]> fileList, long tMin){
        if(tMin <= fileList.get(0)[0]) {
            return 0;
        }
        else{
            int low = 0;
            int high = fileList.size() - 1;
            while(low <= high){
                int mid = (low + high) / 2;
                if(fileList.get(mid)[0] <= tMin && fileList.get(mid)[1] >=tMin){
                    if(mid != 0 && fileList.get(mid-1)[1] == fileList.get(mid)[0]){
                        return mid-1;
                    }
                    else {
                        return mid;
                    }
                }
                else if(tMin <fileList.get(mid)[0]){
                    high = mid - 1;
                }
                else{
                    low = mid + 1;
                }
            }
            return -1;
        }
    }

    public int SearchUpperIndex(List<long[]> fileList, long tMax){
        if(tMax >= fileList.get(fileList.size()-1)[1]) {
            return fileList.size()-1;
        }
        else{
            int low = 0;
            int high = fileList.size() - 1;
            while(low <= high){
                int mid = (low + high) / 2;
                if(fileList.get(mid)[0] <= tMax && fileList.get(mid)[1] >=tMax){
                    if(mid != fileList.size()-1 && fileList.get(mid+1)[0] == fileList.get(mid)[1]){
                        return mid+1;
                    }
                    else {
                        return mid;
                    }
                }
                else if(tMax <fileList.get(mid)[0]){
                    high = mid - 1;
                }
                else{
                    low = mid + 1;
                }
            }
            return -1;
        }
    }
     ***********************************************************************************************/
}
