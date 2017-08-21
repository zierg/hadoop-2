package homework.hadoop;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    static Logger log = LoggerFactory.getLogger(RequestsReducer.class);
}
