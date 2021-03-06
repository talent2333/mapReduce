package flowStatistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/21 14:48
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        String preNum = text.toString().substring(0, 3);
        int partition = 4;
        if ("136".equals(preNum)){
            partition=0;
        }else if ("137".equals(preNum)){
            partition=1;
        }else if ("138".equals(preNum)){
            partition=2;
        }else if ("139".equals(preNum)){
            partition=3;
        }
        return partition;
    }
}
