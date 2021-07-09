package com.tq.hdfs.serials;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.WritableComparable;

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
public class MultiMapWritable implements WritableComparable<MultiMapWritable> {

    private String str;
    private String data;

    public MultiMapWritable(){}

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(str);
        dataOutput.writeUTF(data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.str=dataInput.readUTF();
        this.data=dataInput.readUTF();
    }

    @Override
    public int compareTo(MultiMapWritable o) {

        return this.str.compareTo(o.str);
    }
}
