package io.openmessaging;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ReadFile{

    public List<Message> readFiles(int num){
        List<Message> backList = new ArrayList<>();
        try {
            FileInputStream fis = new FileInputStream(DefaultMessageStoreImpl.PATH + num);
            BufferedInputStream bis = new BufferedInputStream(fis);
            byte[] bt = new byte[DefaultMessageStoreImpl.BUFCAP];
            ByteBuffer buffer = ByteBuffer.wrap(bt);
            int size = bis.available();
            int round = size / DefaultMessageStoreImpl.BUFCAP;
            int mode = size % DefaultMessageStoreImpl.BUFCAP;
            for(int r = 0; r < round; r++) {
                if (bis.read(bt) != -1) {
                    writeToList(buffer, backList, DefaultMessageStoreImpl.BUFCAP);
                }
            }
            if(mode != 0){
                bis.read(bt);
                writeToList(buffer, backList, mode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return backList;
    }

    public void writeToList(ByteBuffer buffer, List<Message> backList, int mode){
        while(buffer.position() != mode) {
            long a = buffer.getLong();
            long t = buffer.getLong();
            byte[] body = new byte[34];
            buffer.get(body);
            backList.add(new Message(a, t, body));
        }
        buffer.clear();
    }

}
