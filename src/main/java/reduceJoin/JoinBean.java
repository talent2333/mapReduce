package reduceJoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author talent2333
 * @Date 2020/5/26 9:11
 */
public class JoinBean implements WritableComparable<JoinBean> {

    private String id;
    private String pid;
    private Integer amount;
    private String name;
    private String flag;

    //反射实例化newInstance()会调用空参构造器
    public JoinBean() {
    }

    public JoinBean(String id, String pid, int amount, String name, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.name = name;
        this.flag = flag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", pid='" + pid + '\'' +
                ", amount=" + amount +
                ", name='" + name + '\'' +
                ", flag=" + flag +
                '}';
    }

    @Override
    public int compareTo(JoinBean o) {
        //顺序比较
        int compare = this.pid.compareTo(o.getPid());
        if (compare == 0) {
            return this.id.compareTo(o.getId());
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(name);
        out.writeUTF(flag);


    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.name = in.readUTF();
        this.flag = in.readUTF();
    }
}
