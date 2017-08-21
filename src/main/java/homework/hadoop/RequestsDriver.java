package homework.hadoop;

import homework.hadoop.mapping.RequestsMapper;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// java -jar homework-2-ip-requests-1.0-SNAPSHOT-all.jar input output
// hadoop jar homework-2-ip-requests-1.0-SNAPSHOT-all.jar input output
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Request Data");
        job.setJarByClass(RequestsDriver.class);
        job.setMapperClass(RequestsMapper.class);
        job.setCombinerClass(RequestsCombiner.class);
        job.setReducerClass(RequestsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TempRequestDataWritable.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        Path outputDir = new Path(strings[1]);
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);
        int result = job.waitForCompletion(true) ? 0 : 1;
        log.info("Browsers usage statistics:");
        job.getCounters()
                .getGroup("Browser Usage")
                .forEach(counter -> log.info("{}: {}", counter.getDisplayName(), counter.getValue()));
        return result;
    }

    public static void main(String[] args) throws Exception {
        RequestsDriver driver = new RequestsDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    static Logger log = LoggerFactory.getLogger(RequestsDriver.class);
}
