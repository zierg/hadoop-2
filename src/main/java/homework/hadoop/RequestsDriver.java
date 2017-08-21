package homework.hadoop;

import homework.hadoop.mapping.RequestsMapper;
import homework.hadoop.writables.RequestDataWritable;
import homework.hadoop.writables.TempRequestDataWritable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * How to use
 * Run in local mode: java -jar homework-2-ip-requests-1.0-SNAPSHOT-all.jar input output
 * Run with the text output format without compression: hadoop jar homework-2-ip-requests-1.0-SNAPSHOT-all.jar input output
 * Run with the sequence output format with compression: hadoop jar homework-2-ip-requests-1.0-SNAPSHOT-all.jar input output compress
 * Read first 40 lines of the output file: hadoop fs -libjars homework-2-ip-requests-1.0-SNAPSHOT-all.jar -text output/part-r-00000 | head -n40
 */

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsDriver extends Configured implements Tool {

    static String COMPRESS_PARAMETER_NAME = "compress";

    public static void main(String[] args) throws Exception {
        RequestsDriver driver = new RequestsDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Request Data");
        job.setJarByClass(RequestsDriver.class);
        job.setMapperClass(RequestsMapper.class);
        job.setCombinerClass(RequestsCombiner.class);
        job.setReducerClass(RequestsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TempRequestDataWritable.class);
        job.setOutputValueClass(RequestDataWritable.class);
        setupFileSystem(conf, job, args);
        int result = job.waitForCompletion(true) ? 0 : 1;
        log.info("Browsers usage statistics:");
        job.getCounters()
                .getGroup("Browser Usage")
                .forEach(counter -> log.info("{}: {}", counter.getDisplayName(), counter.getValue()));
        return result;
    }

    private void setupFileSystem(Configuration conf, Job job, String[] args) throws IOException {
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        clearLocalSystem(conf, outputDir);
        if (args.length >= 3 && COMPRESS_PARAMETER_NAME.equals(args[2])) {
            setupForSequenceFileWithCompression(job, outputDir);
        } else {
            setupForTextFileWithoutCompression(conf, job, outputDir);
        }
    }

    private void setupForSequenceFileWithCompression(Job job, Path outputDir) {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputDir);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    }

    private void setupForTextFileWithoutCompression(Configuration conf, Job job, Path outputDir) {
        FileOutputFormat.setOutputPath(job, outputDir);
    }

    private void clearLocalSystem(Configuration conf, Path outputDir) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDir, true);
    }

    static Logger log = LoggerFactory.getLogger(RequestsDriver.class);
}
