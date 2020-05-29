package commonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description MyFriends
 * @Author talent2333
 * @Date 2020/5/27 22:52
 */
public class FriendDriver {

    public static void main(String[] args) throws Exception{

        args = new String[]{"d:/data/friends.txt","d:/output/2"};
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(FriendDriver.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        //
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
