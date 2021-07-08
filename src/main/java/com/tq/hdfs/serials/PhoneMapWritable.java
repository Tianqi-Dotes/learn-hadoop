package com.tq.hdfs.serials;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

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
public class PhoneMapWritable implements Writable {

    private Integer upCount;
    private Integer downCount;

    public PhoneMapWritable(){
    }

    public PhoneMapWritable(Integer upCount,Integer downCount){
        this.upCount=upCount;
        this.downCount=downCount;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upCount);
        dataOutput.writeInt(downCount);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upCount=dataInput.readInt();
        this.downCount=dataInput.readInt();
    }
}
