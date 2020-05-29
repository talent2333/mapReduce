package groupingComparatorOrder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/23 15:02
 */
public class OrderBean implements WritableComparable<OrderBean> {


    private int id;
    private double price;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", price=" + price +
                '}';
    }

    @Override
    public int compareTo(OrderBean o) {
        if (this.id > o.getId()) {
            return 1;
        } else if (this.id<o.getId()){
            return -1;
        } else {
            return this.price>o.getPrice()?-1:1;
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.price = in.readDouble();
    }
}
