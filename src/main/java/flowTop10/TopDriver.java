package flowTop10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 22:24
 */
public class TopDriver {

    public static void main(String[] args) throws Exception{

        args = new String[]{"d:/data/phone_data.txt","d:/output/4"};

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(TopDriver.class);

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TopBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1) ;

    }
}
