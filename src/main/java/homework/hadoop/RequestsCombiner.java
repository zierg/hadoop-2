package homework.hadoop;

import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsCombiner extends Reducer<Text, TempRequestDataWritable, Text, TempRequestDataWritable> {

    @Override
    protected void reduce(Text key, Iterable<TempRequestDataWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        long amount = 0;
        for (val currentData : values) {
            total += currentData.getTotalBytes().get();
            amount += currentData.getAmountOfRequests().get();
        }
        requestData.setAmountOfRequests(amount);
        requestData.setTotalBytes(total);
        context.write(key, requestData);
    }

    TempRequestDataWritable requestData = new TempRequestDataWritable();

    static Logger log = LoggerFactory.getLogger(RequestsCombiner.class);
}
