package reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author talent2333
 * @Date 2020/5/26 9:10
 */
public class JoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/data/join", "d:/output/3"};

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(reduceJoin.JoinDriver.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducerer.class);

        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        //确认key
        job.setGroupingComparatorClass(JoinComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
