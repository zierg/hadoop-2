package test.homework.hadoop;

import homework.hadoop.RequestDataWritable;
import homework.hadoop.RequestsMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class RequestsMapperTest {

    @Test
    public void test() throws IOException {
        Text value = new Text(TEST);
        new MapDriver<Object, Text, Text, RequestDataWritable>()
                .withMapper(new RequestsMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(OUTPUT_1_KEY, OUTPUT_1_VALUE)
                .withOutput(OUTPUT_2_KEY, OUTPUT_2_VALUE)
                .runTest(false);
    }

    private static final Text OUTPUT_1_KEY = new Text("ip1");
    private static final Text OUTPUT_2_KEY = new Text("ip2");

    private static final RequestDataWritable OUTPUT_1_VALUE = new RequestDataWritable()
            .setAverageBytes(40028)
            .setTotalBytes(40028);

    private static final RequestDataWritable OUTPUT_2_VALUE = new RequestDataWritable()
            .setAverageBytes(14917)
            .setTotalBytes(14917);

    private static final String TEST =
            "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
            "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
}