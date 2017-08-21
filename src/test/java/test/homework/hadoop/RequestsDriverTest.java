package test.homework.hadoop;

import homework.hadoop.RequestsCombiner;
import homework.hadoop.RequestsReducer;
import homework.hadoop.mapping.RequestsMapper;
import homework.hadoop.writables.RequestDataWritable;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsDriverTest {

    @Test
    public void testMapReduce() throws IOException {
        MapReduceDriver.<Object, Text, Text, TempRequestDataWritable, Text, RequestDataWritable>newMapReduceDriver()
                .withMapper(new RequestsMapper())
                .withCombiner(new RequestsCombiner())
                .withReducer(new RequestsReducer())
                .withCounter(GROUP_NAME, SPIDER, SPIDER_AMOUNT)
                .withCounter(GROUP_NAME, FIREFOX_BROWSER, FIREFOX_AMOUNT)
                .withInput(new LongWritable(0), new Text(TEST))
                .withOutput(new Text(IP_1), IP_1_EXPECTED)
                .withOutput(new Text(IP_2), IP_2_EXPECTED)
                .runTest(false);
    }

    static String TEST =
            "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
                    "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
                    "ip1 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 42011 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
                    "ip1 - - [24/Apr/2011:04:18:54 -0400] \"GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1\" 200 6244 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 14917 \"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss20/floppy.jpg HTTP/1.1\" 200 12433 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ss5_1.jpg HTTP/1.1\" 200 14675 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ss5_jumpers.jpg HTTP/1.1\" 200 39884 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"\n" +
                    "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ss5_server.jpg HTTP/1.1\" 200 51920 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";

    static String IP_1 = "ip1";
    static String IP_2 = "ip2";

    static long TOTAL_IP_1 = 40028 + 56928 + 42011 + 6244;
    static float AVERAGE_IP_1 = getAverageBytes(TOTAL_IP_1, 4);

    static long TOTAL_IP_2 = 14917 + 390 + 12433 + 14675 + 39884 + 51920;
    static float AVERAGE_IP_2 = getAverageBytes(TOTAL_IP_2, 6);

    static RequestDataWritable IP_1_EXPECTED = new RequestDataWritable()
            .setAverageBytes(AVERAGE_IP_1)
            .setTotalBytes(TOTAL_IP_1);

    static RequestDataWritable IP_2_EXPECTED = new RequestDataWritable()
            .setAverageBytes(AVERAGE_IP_2)
            .setTotalBytes(TOTAL_IP_2);

    static String GROUP_NAME = "Browser Usage";
    static String FIREFOX_BROWSER = "Firefox";
    static String SPIDER = "Robot/Spider";
    static int FIREFOX_AMOUNT = 6;
    static int SPIDER_AMOUNT = 4;

    private static float getAverageBytes(double total, double amount) {
        return (float) (total / amount);
    }
}