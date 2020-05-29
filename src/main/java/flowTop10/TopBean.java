package flowTop10;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description TopBean
 * @Author talent2333
 * @Date 2020/5/27 10:08
 */
public class TopBean  implements Writable{

    Long totalFlow;

    public TopBean() {
        super();
    }

    public TopBean(Long totalFlow) {
        super();
        this.totalFlow = totalFlow;
    }

    public Long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(Long totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override
    public String toString() {
        return "TopBean{" +
                "totalFlow=" + totalFlow +
                '}';
    }

//    @Override
//    public int compareTo(TopBean o) {
//
//        return -this.totalFlow.compareTo(o.getTotalFlow());
//    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.totalFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.totalFlow = in.readLong();
    }
}
