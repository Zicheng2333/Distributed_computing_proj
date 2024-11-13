package mapper;

import java.io.IOException;

//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    private String securityID;
    private long time1;
    private long time2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        securityID = context.getConfiguration().get("securityID");
        time1 = Long.parseLong(context.getConfiguration().get("time1"));
        time2 = Long.parseLong(context.getConfiguration().get("time2"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        String dataType = context.getConfiguration().get("dataType");

        if ("order".equals(dataType)){
            String currentSecurityID = fields[8].trim(); //提取第九列，去除字符串首尾的空格
            if (securityID.equals(currentSecurityID)){
                String applSeqNum = fields[7].trim();
                String transactTime = fields[12].trim();
                context.write(new Text(applSeqNum), new Text("O|"+transactTime));
            }
        } else if ("trade".equals(dataType)) {

        }


    }
}