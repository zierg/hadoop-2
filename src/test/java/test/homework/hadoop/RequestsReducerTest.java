package test.homework.hadoop;

import homework.hadoop.RequestsReducer;
import homework.hadoop.writables.RequestDataWritable;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsReducerTest {
    
    @Test
    public void testRequestDataReducing() throws IOException {
        new ReduceDriver<Text, TempRequestDataWritable, Text, RequestDataWritable>()
                .withReducer(new RequestsReducer())
                .withInput(new Text(IP_1), Arrays.asList(IP_1_REQUEST_1, IP_1_REQUEST_2))
                .withInput(new Text(IP_2), Collections.singletonList(IP_2_REQUEST))
                .withOutput(new Text(IP_1), IP_1_EXPECTED)
                .withOutput(new Text(IP_2), IP_2_EXPECTED)
                .runTest(false);
    }
    
    static String IP_1 = "ip1";
    static String IP_2 = "ip2";

    static long IP_1_DATA_1_BYTES = 320;
    static long IP_1_DATA_2_BYTES = 128;
    static long IP_1_DATA_1_AMOUNT = 1;
    static long IP_1_DATA_2_AMOUNT = 1;
    
    static TempRequestDataWritable IP_1_REQUEST_1 = new TempRequestDataWritable()
            .setAmountOfRequests(IP_1_DATA_1_AMOUNT)
            .setTotalBytes(IP_1_DATA_1_BYTES);

    static TempRequestDataWritable IP_1_REQUEST_2 = new TempRequestDataWritable()
            .setAmountOfRequests(IP_1_DATA_2_AMOUNT)
            .setTotalBytes(IP_1_DATA_2_BYTES);
    
    static RequestDataWritable IP_1_EXPECTED = new RequestDataWritable()
            .setAverageBytes(getAverageBytes(IP_1_DATA_1_BYTES + IP_1_DATA_2_BYTES, 
                                             IP_1_DATA_1_AMOUNT + IP_1_DATA_2_AMOUNT))
            .setTotalBytes(IP_1_DATA_1_BYTES + IP_1_DATA_2_BYTES);

    static long IP_2_DATA_BYTES = 5696;
    static long IP_2_DATA_AMOUNT = 5;

    static TempRequestDataWritable IP_2_REQUEST = new TempRequestDataWritable()
            .setAmountOfRequests(IP_2_DATA_AMOUNT)
            .setTotalBytes(IP_2_DATA_BYTES);

    static RequestDataWritable IP_2_EXPECTED = new RequestDataWritable()
            .setAverageBytes(getAverageBytes(IP_2_DATA_BYTES, IP_2_DATA_AMOUNT))
            .setTotalBytes(IP_2_DATA_BYTES);

    private static float getAverageBytes(double total, double amount) {
        return (float) (total / amount);
    }
}