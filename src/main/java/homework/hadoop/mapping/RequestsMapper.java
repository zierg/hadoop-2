package homework.hadoop.mapping;

import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import static homework.hadoop.mapping.ParamsExtractor.getLogRecordParams;
import static homework.hadoop.mapping.UserAgentUtils.getBrowser;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsMapper extends Mapper<Object, Text, Text, TempRequestDataWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            processLogRecord(itr.nextToken(), context);
        }
    }

    private void processLogRecord(String logRecord, Context context) throws IOException, InterruptedException {
        String[] params = getLogRecordParams(logRecord);
        try {
            ip.set(params[0]);
            int bytes = getBytes(params[1]);
            requestData.setTotalBytes(bytes);
            context.write(ip, requestData);
            context.getCounter(BROWSER_USAGE_GROUP, getBrowser(params[2])).increment(1);
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new RuntimeException("Wrong! log = " + logRecord + ", array = " + Arrays.toString(params));
        }
    }

    private int getBytes(String bytesString) {
        return StringUtils.isNumeric(bytesString) ? Integer.parseInt(bytesString) : 0;
    }

    Text ip = new Text();

    TempRequestDataWritable requestData = new TempRequestDataWritable()
            .setAmountOfRequests(1);

    static String BROWSER_USAGE_GROUP = "Browser Usage";

    static Logger log = LoggerFactory.getLogger(RequestsMapper.class);
}
