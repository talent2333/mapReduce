package logTest;

import groupingComparatorOrder.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 15:34
 */
public class LogDriver {

    public static void main(String[] args) throws Exception{

        args = new String[]{"d:/data/web.log","d:/output/1"};

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }
}
