package homework.hadoop;

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
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        RequestsDriver driver = new RequestsDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

//    public static void main(String[] args) {
//
//
//        String logRecord = "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
//        String[] strings = logRecord.replaceAll("(.+) - -.*\\[.+] \"[^\"]+\" \\d+ (\\d+) \"[^\"]+\" \"([^\"]+)\"", "$1;$2;$3").split(";");
//        log.info("Strings: {}", Arrays.toString(strings));
//
//        UserAgent userAgent = UserAgent.parseUserAgentString(strings[2]);
//        log.info("Browser: {}", userAgent.getBrowser().getGroup().getName());
//    }

    static Logger log = LoggerFactory.getLogger(RequestsDriver.class);
}
