import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
    public static void main(String[] args) {


        Map<String, Integer> map = new HashMap<>();

        map.put("topic-0", 11);

        HashMap<String, List<String>> listHashMap = new HashMap<>();


        List<String> topicList = new ArrayList<>();

        topicList.add("topic-0");

        listHashMap.put("c1",topicList);
        listHashMap.put("c2",topicList);
        listHashMap.put("c3",topicList);

        RangeAssignor rangeAssignor = new RangeAssignor();
        Map<String, List<TopicPartition>> assign = rangeAssignor.assign(map, listHashMap);

        for (Map.Entry<String, List<TopicPartition>> entry : assign.entrySet()) {
            for (TopicPartition partition : entry.getValue()) {
                System.out.println(entry.getKey() + ":"+ partition.toString());
            }
        }

    }
}
