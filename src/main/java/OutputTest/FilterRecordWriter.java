package OutputTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/24 11:464
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream xtyOut = null;
    private FSDataOutputStream otherOut = null;
    //创建
    public FilterRecordWriter(TaskAttemptContext job) {

        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建输出文件路径
            Path xtyPath = new Path("d:/output/1/xty.txt");
            Path otherPath = new Path("d:/output/1/other.txt");
            //创建输出流
            xtyOut = fs.create(xtyPath);
            otherOut = fs.create(otherPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        //过滤字段输出到不同文件
        if(key.toString().contains("xitianyu")){
            xtyOut.write(key.toString().getBytes());
        }else{
            otherOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        //关闭资源
        IOUtils.closeStream(xtyOut);
        IOUtils.closeStream(otherOut);
    }
}
