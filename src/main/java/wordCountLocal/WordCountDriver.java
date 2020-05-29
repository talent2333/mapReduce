package wordCountLocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/20 11:02
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        args = new String[]{"d:/data/splits/1.txt","d:/output/1"};
//        args = new String[]{"d:/data/split_data.txt","d:/output/1"};
        args = new String[]{"d:/data/copy.txt","d:/output/5"};

        Configuration config = new Configuration();
//        config.set("mapreduce.reduce.shuffle.input.buffer.percent","0.1");
////         开启map端输出压缩
//        config.set("mapreduce.map.output.compress", "true");
//         设置map端输出压缩方式
//        config.set("mapreduce.map.output.compress.codec",
//                "org.apache.hadoop.io.compress.DefaultCodec");
        //设置reduce端输出压缩
//        config.set("mapreduce.output.fileoutputformat.compress","true");
//        config.set("mapreduce.output.fileoutputformat.compress.codec",
//                "org.apache.hadoop.io.compress.DefaultCodec");

        Job job = Job.getInstance(config);
        //设置jar加载路径
        job.setJarByClass(wordCountLocal.WordCountDriver.class);
        //设置map和reduce类
        job.setMapperClass(wordCountLocal.WordCountMapper.class);
        job.setReducerClass(wordCountLocal.WordCountReducer.class);
        //设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置小文件整合后切片
//        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置分区的虚拟存储大小
//        CombineTextInputFormat.setMaxInputSplitSize(job,10000000);
        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置分区
//        job.setPartitionerClass(WordCountPartitioner.class);
//        job.setNumReduceTasks(2);

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }

}
