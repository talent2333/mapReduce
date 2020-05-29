package wordCountLocal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Test;

import java.util.Comparator;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/23 9:48
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    /**
     * @Description
     * @param text  切分后的字符串
     * @param intWritable
     * @param numPartitions
     * @Return int
     * @Date 2020/5/23 9:48
     */
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        //设置分区号
        int partition = 0;
        //取字符串首字符
        String str_prefix = text.toString().substring(0,1);

        if ("q".compareTo(str_prefix) > 0){
            partition = 1;
        }
        return partition;
    }
    @Test
    public void test1(){
        String s1 = "";
        System.out.println(s1.equals(""));
    }
}
