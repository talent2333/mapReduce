package reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description 通用join方法，问题：数据倾斜
 * @Author talent2333
 * @Date 2020/5/26 9:09
 * @Data
 * order
 * id   pid amount
 * 1001 01	1
 * 1002	02	2
 * 1003	03	3
 * 1004	01	4
 * 1005	02	5
 * 1006	03	6
 *******************
 * pd
 * pid  name
 * 01	小米
 * 02	华为
 * 03	格力
 */
public class JoinMapper extends Mapper<LongWritable, Text, JoinBean, NullWritable> {

    private String fileName;
    private JoinBean bean = new JoinBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获得切片信息
        FileSplit fs = (FileSplit) context.getInputSplit();
        fileName = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //切割每行数据
        String[] fields = value.toString().split("\t");
        if ("order.txt".equals(fileName)) {
            bean.setId(fields[0]);
            bean.setPid(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setName("");
            bean.setFlag("order");

        } else if ("pd.txt".equals(fileName)) {
            bean.setId("");
            bean.setPid(fields[0]);
            bean.setAmount(0);
            bean.setName(fields[1]);
            bean.setFlag("pd");
        }
        context.write(bean, NullWritable.get());


    }

    @Test
    public void test001() {
        String a = "";
        String b = "01";
        int i = a.compareTo(b);
        System.out.println(i);
    }

}
