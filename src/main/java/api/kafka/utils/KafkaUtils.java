package api.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaUtils {

    public static String listToString(List<String>bootstrapServersList){
       String bootstrapServers="";
        if(bootstrapServersList!=null && !bootstrapServersList.isEmpty()) {
            for (int i = 0; i < bootstrapServersList.size(); i++) {
                bootstrapServers =bootstrapServers+bootstrapServersList.get(i);
                if(i<bootstrapServersList.size()-1){
                    bootstrapServers+=",";
                }

            }
        }
        return bootstrapServers;

    }

    public static <T> ArrayList<T> convertIteratorToArrayList(Iterator<T> iterator) {
        ArrayList<T> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public static ConcurrentHashMap<String,List<String>> convertHashMap(List<ConsumerRecord> list, ConcurrentHashMap<String,List<String>> hashMap){
        List<String> values;
        String topic;
        for(int i=0;i< list.size();i++){
            topic = list.get(i).topic();
            if(hashMap.containsKey(topic)){
                values = hashMap.get(topic);
                values.add((String)list.get(i).value());
            }
            else{
                values = new ArrayList<>();
                values.add((String)list.get(i).value());
                hashMap.put(topic,values);
            }
        }
        return hashMap;
    }
}
