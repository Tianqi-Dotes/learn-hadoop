package com.tq.hdfs.serials;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tq
 * @version V1.0
 * @Package com.tq.hdfs.serials
 * @date 2021-06-21 17:29
 * @Copyright © 2018-2019 *******
 */
@Getter
@Setter
@ToString
public class PhoneReduceWritable implements Writable {

    private Integer upTotal;
    private Integer downTotal;
    private Integer total;

    public PhoneReduceWritable(){}
    public PhoneReduceWritable(Integer upTotal,Integer downTotal,Integer total){
        this.upTotal=upTotal;
        this.downTotal=downTotal;
        this.total=total;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upTotal);
        dataOutput.writeInt(downTotal);
        dataOutput.writeInt(total);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upTotal=dataInput.readInt();
        this.downTotal=dataInput.readInt();
        this.total=dataInput.readInt();
    }
}
