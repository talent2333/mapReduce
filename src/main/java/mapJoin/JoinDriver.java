package mapJoin;

import groupingComparatorOrder.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description mapjoin
 * @Author talent2333
 * @Date 2020/5/26 11:47
 */
public class JoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        args = new String[]{"d:/data/join/order.txt","d:/output1"};

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        //设置文件缓存
        job.addCacheFile(new URI("file:///d:/data/join/pd.txt"));
//        job.addCacheFile(new URI("file://"+args[0]));

        job.setJarByClass(JoinDriver.class);

        job.setMapperClass(JoinMapper.class);
//        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduceTask=0，不需要经过shuffle&&reducer
        job.setNumReduceTasks(0);

//        job.setOutputKeyClass(OrderBean.class);
//        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置reduce分组
        job.setGroupingComparatorClass(OrderWritableComparator.class);

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
