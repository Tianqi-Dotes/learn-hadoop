package com.tq.hdfs.serials;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.serials
 * @date 2021-07-05 10:17
 * @Copyright © 2018-2019 *******
 */
@AllArgsConstructor
@ToString
@Getter
@Setter
public class OrderJoinWritable implements Writable {

    private Integer orderId;
    private String pId;
    private Integer amount;
    private String pName;
    private String flag;

    public OrderJoinWritable(){}

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(orderId);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {

        this.orderId=dataInput.readInt();
        this.pId=dataInput.readUTF();
        this.amount=dataInput.readInt();
        this.pName=dataInput.readUTF();
        this.flag=dataInput.readUTF();
    }
}
