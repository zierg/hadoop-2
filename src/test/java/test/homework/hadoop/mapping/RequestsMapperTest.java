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
                .withCounter(GROUP_NAME, ROBOT_SPIDER, ROBOT_SPIDER_COUNT)
                .withCounter(GROUP_NAME, FIREFOX, FIREFOX_COUNT)
                .withOutput(OUTPUT_1_KEY, OUTPUT_1_VALUE)
                .withOutput(OUTPUT_2_KEY, OUTPUT_2_VALUE)
                .withOutput(OUTPUT_3_KEY, OUTPUT_3_VALUE)
                .withOutput(OUTPUT_4_KEY, OUTPUT_4_VALUE)
                .runTest(false);
    }

    static Text OUTPUT_1_KEY = new Text("ip1");
    static Text OUTPUT_2_KEY = new Text("ip2");
    static Text OUTPUT_3_KEY = new Text("ip28");
    static Text OUTPUT_4_KEY = new Text("ip399");

    static TempRequestDataWritable OUTPUT_1_VALUE = new TempRequestDataWritable()
            .setTotalBytes(40028)
            .setAmountOfRequests(1);

    static TempRequestDataWritable OUTPUT_2_VALUE = new TempRequestDataWritable()
            .setTotalBytes(14917)
            .setAmountOfRequests(1);

    static TempRequestDataWritable OUTPUT_3_VALUE = new TempRequestDataWritable()
            .setTotalBytes(0)
            .setAmountOfRequests(1);

    static TempRequestDataWritable OUTPUT_4_VALUE = new TempRequestDataWritable()
            .setTotalBytes(0)
            .setAmountOfRequests(1);

    static String TEST =
            "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip28 - - [24/Apr/2011:05:41:56 -0400] \"GET /sun3/ HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"\n" +
                    "ip399 - - [25/Apr/2011:04:12:25 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; ru; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11\"";

    static String GROUP_NAME = "Browser Usage";
    static String ROBOT_SPIDER = "Robot/Spider";
    static int ROBOT_SPIDER_COUNT = 2;
    static String FIREFOX = "Firefox";
    static int FIREFOX_COUNT = 2;
}