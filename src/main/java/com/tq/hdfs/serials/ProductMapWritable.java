package com.tq.hdfs.serials;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.serials
 * @date 2021-06-21 17:28
 * @Copyright © 2018-2019 *******
 */
@Getter
@Setter
@ToString
public class ProductMapWritable implements WritableComparable<ProductMapWritable> {

    private String orderId;
    private String proId;
    private Double price;

    public ProductMapWritable(){
    }
    public ProductMapWritable(String orderId,String proId, Double price){
        this.orderId=orderId;
        this.proId=proId;
        this.price=price;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(proId);
        dataOutput.writeDouble(price);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.proId=dataInput.readUTF();
        this.price=dataInput.readDouble();
    }

    public int compareTo(ProductMapWritable o) {
        int orderId=this.getOrderId().compareTo(o.getOrderId());
        if(orderId!=0) {
            return orderId;
        }
        return -this.price.compareTo(o.getPrice());
    }
}
