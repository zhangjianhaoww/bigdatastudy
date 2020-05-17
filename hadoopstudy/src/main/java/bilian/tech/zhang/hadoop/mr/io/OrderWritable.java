package bilian.tech.zhang.hadoop.mr.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * key
 * 实现 WritableComparable 接口
 * 可以实现排序功能
 * 二次排序（将需要排序的字段封装成类，根据 compareTo() 方法编写排序逻辑）
 *
 */
public class OrderWritable implements WritableComparable<OrderWritable> {

    private int orderId;
    private float price;

    public void setData(int orderId, float price) {
        this.orderId = orderId;
        this.price = price;
    }

    public OrderWritable() {
    }

    public OrderWritable(int orderId, float price) {
        setData(orderId, price);
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderWritable o) {
        int compare = Integer.valueOf(orderId).compareTo(o.orderId);
        if (compare == 0){
            Float.valueOf(price).compareTo(o.price);
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(orderId);
        out.writeFloat(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.orderId = in.readInt();
        this.price = in.readFloat();
    }


    @Override
    public String toString() {
        return "OrderWritable{" +
                "orderId=" + orderId +
                ", price=" + price +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderWritable that = (OrderWritable) o;
        return orderId == that.orderId &&
                Float.compare(that.price, price) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, price);
    }
}
