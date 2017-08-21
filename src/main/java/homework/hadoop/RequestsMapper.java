package homework.hadoop;

import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

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
        ip.set(params[0]);
        int bytes = Integer.parseInt(params[1]);
        requestData.setTotalBytes(bytes);
        context.write(ip, requestData);
    }

    /**
     * Parses a log record
     * @param logRecord the log record
     * @return the log record's params in an array in the following form [ip, bytes, user_agent]
     */
    private String[] getLogRecordParams(String logRecord) {
        return logRecord.replaceAll(MAPPING_REGEX, MAPPING_REPLACEMENT).split(MAPPING_DELIMITER);
    }

    Text ip = new Text();

    TempRequestDataWritable requestData = new TempRequestDataWritable()
            .setAmountOfRequests(1);

    static String MAPPING_REGEX = "(.+) - -.*\\[.+] \"[^\"]+\" \\d+ (\\d+) \"[^\"]+\" \"([^\"]+)\"";
    static String MAPPING_DELIMITER = ";";
    static String MAPPING_REPLACEMENT = "$1" + MAPPING_DELIMITER + "$2" + MAPPING_DELIMITER + "$3";

    static Logger log = LoggerFactory.getLogger(RequestsMapper.class);
}
