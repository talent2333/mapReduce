package indexSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

/**
 * @Description index sort task
 * @Author talent2333
 * @Date 2020/5/26 19:23
 */
public class IndexDriver {

    public static void main(String[] args) throws Exception{

        args = new String[]{"d:/data/index","d:/output/8"};

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(IndexDriver.class);

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducerer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //set combiner
        job.setCombinerClass(IndexReducer.class);
        //setGroupingComparator
        job.setGroupingComparatorClass(IndexComparator.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }

}
