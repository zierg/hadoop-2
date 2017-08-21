package homework.hadoop;

import homework.hadoop.writables.RequestDataWritable;
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
public class RequestsReducer extends Reducer<Text, TempRequestDataWritable, Text, RequestDataWritable> {

    @Override
    protected void reduce(Text key, Iterable<TempRequestDataWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        long amount = 0;
        for (val currentData : values) {
            total += currentData.getTotalBytes().get();
            amount += currentData.getAmountOfRequests().get();
        }
        float averageBytes = getAverageBytes(total, amount);
        requestData.setAverageBytes(averageBytes);
        requestData.setTotalBytes(total);
        context.write(key, requestData);
    }

    private float getAverageBytes(double total, double amount) {
        return (float) (total / amount);
    }

    RequestDataWritable requestData = new RequestDataWritable();

    static Logger log = LoggerFactory.getLogger(RequestsReducer.class);
}
