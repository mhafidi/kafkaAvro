package api.kafka.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKafkaUtils {

    List<String> ipAddresses = Arrays.asList("192.168.1.1:8080", "10.0.0.1:8081", "172.16.0.1:8082");
    String concatIpAddresses="192.168.1.1:8080,10.0.0.1:8081,172.16.0.1:8082";
    @Test
    public void testlistToString(){
        assertEquals(KafkaUtils.listToString(ipAddresses),concatIpAddresses);
        assertEquals(KafkaUtils.listToString(null),"");
    }

}
