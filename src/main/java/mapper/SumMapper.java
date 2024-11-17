package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
// 该类为合并同索引主动单的mapper
public class SumMapper extends Mapper < LongWritable , Text , Text , Text > {

    private Text ApplSeqNum = new Text(); //主动单索引

    public void map( LongWritable key , Text value , Context context)
            throws IOException , InterruptedException {
        String[] parts = value. toString(). split("\t");

        if (Integer.parseInt(parts[4]) == 0) {
            ApplSeqNum.set(parts[1]);
        }
        else if (Integer.parseInt(parts[4]) == 1) {
            ApplSeqNum.set(parts[0]);
        }

        String infos = parts[2] + "\t" + parts[3] + "\t" + parts[4];
        context.write(ApplSeqNum,new Text(infos));
    }
}
