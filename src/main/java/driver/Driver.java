package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.checkerframework.checker.units.qual.C;
import reducer.Reducer;
import mapper.Mapper;

//TODO example usage:
// hadoop jar OrderTradeProcessor.jar OrderTradeDriver \
//    /input/order_data /output/order_result order 000016 20190102091500000 20190102113000073
//  hadoop jar OrderTradeProcessor.jar OrderTradeDriver \
//    /input/trade_data /output/trade_result trade 000016 20190102091500000 20190102113000073

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length<6){
            System.err.println("Usage: OrderTradeDriver <input path> <output path> <data type> <securityID> <time1> <time2>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("dataType",args[2]);
        conf.set("securityID",args[3]);
        conf.set("time1",args[4]);
        conf.set("time2",args[5]);

        Job job = Job.getInstance(conf,"Order and Trade Processor");
        job.setJarByClass(Driver.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
