package test.homework.hadoop.mapping;

import homework.hadoop.mapping.RequestsMapper;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsMapperTest {

    @Test
    public void test() throws IOException {
        Text value = new Text(TEST);
        new MapDriver<Object, Text, Text, TempRequestDataWritable>()
                .withMapper(new RequestsMapper())
                .withInput(new LongWritable(0), value)
                .withCounter("Browser Usage", "Robot/Spider", 1)
                .withCounter("Browser Usage", "Firefox", 1)
                .withOutput(OUTPUT_1_KEY, OUTPUT_1_VALUE)
                .withOutput(OUTPUT_2_KEY, OUTPUT_2_VALUE)
                .runTest(false);
    }

    static Text OUTPUT_1_KEY = new Text("ip1");
    static Text OUTPUT_2_KEY = new Text("ip2");

    static TempRequestDataWritable OUTPUT_1_VALUE = new TempRequestDataWritable()
            .setTotalBytes(40028)
            .setAmountOfRequests(1);

    static TempRequestDataWritable OUTPUT_2_VALUE = new TempRequestDataWritable()
            .setTotalBytes(14917)
            .setAmountOfRequests(1);

    static String TEST =
            "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
            "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
}