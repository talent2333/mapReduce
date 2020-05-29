package flowTotal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/21 19:22
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        String preNum = text.toString().substring(0, 3);

        int partition = 4;
        switch (preNum) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139": {
                partition = 3;
            }
            break;
        }
        return partition;
    }
}
