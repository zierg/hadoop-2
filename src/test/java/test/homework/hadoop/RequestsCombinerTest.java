package test.homework.hadoop;

import homework.hadoop.RequestsCombiner;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsCombinerTest {
    
    @Test
    public void testRequestDataCombining() throws IOException {
        new ReduceDriver<Text, TempRequestDataWritable, Text, TempRequestDataWritable>()
                .withReducer(new RequestsCombiner())
                .withInput(new Text(IP), Arrays.asList(REQUEST_1, REQUEST_2))
                .withOutput(new Text(IP), EXPECTED)
                .runTest(false);
    }

    static String IP = "ip1";
    static long DATA_1_BYTES = 2569;
    static long DATA_2_BYTES = 5248;
    static long DATA_1_AMOUNT = 15;
    static long DATA_2_AMOUNT = 2;

    static TempRequestDataWritable REQUEST_1 = new TempRequestDataWritable()
            .setAmountOfRequests(DATA_1_AMOUNT)
            .setTotalBytes(DATA_1_BYTES);

    static TempRequestDataWritable REQUEST_2 = new TempRequestDataWritable()
            .setAmountOfRequests(DATA_2_AMOUNT)
            .setTotalBytes(DATA_2_BYTES);

    static TempRequestDataWritable EXPECTED = new TempRequestDataWritable()
            .setAmountOfRequests(DATA_1_AMOUNT + DATA_2_AMOUNT)
            .setTotalBytes(DATA_1_BYTES + DATA_2_BYTES);

}