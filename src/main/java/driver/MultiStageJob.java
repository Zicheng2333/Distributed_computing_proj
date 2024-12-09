package driver;

import mapper.*;
import reducer.FinalAggregationReducer; // 新增的最终归并Reducer
import utils.TimeWindowUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MultiStageJob {

    private static BufferedWriter logWriter = null;

    private static void logMessage(String msg) throws IOException {
        long currentTime = System.currentTimeMillis();
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedTime = sdf.format(new java.util.Date(currentTime));
        String logStr = "[" + formattedTime + "] " + msg;
        System.out.println(logStr);
        if (logWriter != null) {
            logWriter.write(logStr);
            logWriter.newLine();
            logWriter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        // 初始化日志文件
        try {
            logWriter = new BufferedWriter(new FileWriter("multi_stage_timing.log", true));
        } catch (IOException e) {
            System.err.println("Failed to open log file for writing.");
            System.exit(1);
        }

        if (args.length != 8) {
            System.err.println("Usage: MultiStageJob <orderInputPath> <tradeInputPath> <intermediateOutputPath1> "
                    + "<intermediateOutputPath2>  <intermediateTradeOutputBasePath> <finalOutputPath> <securityID> "
                    + "<circulatingStock> <interval>");
            System.exit(-1);
        }

        // 获取参数
        String orderInputPath = args[0];
        String tradeInputPath = args[1];
        String intermediateOutputPath1 = args[2];
        //String intermediateOutputPath2 = args[3]; // 不再用这个作为多窗口输出目录，可以作为中间目录使用
        String intermediateTradeOutputBasePath = args[3];
        String finalOutputPath = args[4];
        String securityID = args[5];
        String circulatingStock = args[6];
        String interval = args[7];

        List<String> timeWindows = TimeWindowUtils.generateTimeWindows(interval);

        logMessage("Processing order data");

        // Step 1: Job to process order data (不变)
        Configuration conf1 = new Configuration();
        conf1.set("securityID", securityID);
        Job job1 = Job.getInstance(conf1, "Process Order Data");
        job1.setJarByClass(MultiStageJob.class);
        job1.setMapperClass(OrderMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job1, new Path(orderInputPath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutputPath1));

        if (!job1.waitForCompletion(true)) {
            logMessage("Job 1 failed");
            System.exit(1);
        }

        // Step 2: 预处理交易数据
        // 将数据按照timeWindow输出 (timeWindow, record)
        StringBuilder sb = new StringBuilder();
        for (String tw : timeWindows) {
            sb.append(tw).append(",");
        }

        Configuration confPre = new Configuration();
        confPre.set("securityID", securityID);
        confPre.set("timeWindows", sb.toString());

        logMessage("Start preprocessing trade data");

        Job preprocessJob = Job.getInstance(confPre, "Preprocess Trade Data by Windows");
        preprocessJob.setJarByClass(MultiStageJob.class);
        preprocessJob.setMapperClass(TradePreprocessingMapper.class);
        // 无Reduce任务，直接输出 (timeWindow, 原始记录)
        preprocessJob.setMapOutputKeyClass(Text.class);
        preprocessJob.setMapOutputValueClass(Text.class);
        preprocessJob.setOutputKeyClass(Text.class);
        preprocessJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(preprocessJob, new Path(tradeInputPath));
        FileOutputFormat.setOutputPath(preprocessJob, new Path(intermediateTradeOutputBasePath));

        if (!preprocessJob.waitForCompletion(true)) {
            logMessage("Trade Preprocessing Job failed");
            System.exit(1);
        }

        logMessage("Adding cache files");

        // 准备Cache Files (order映射文件)
        Configuration cacheConf = new Configuration();
        FileSystem fs = FileSystem.get(cacheConf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(intermediateOutputPath1));
        List<URI> cacheFiles = new ArrayList<>();
        for (FileStatus status : fileStatuses) {
            String filename = status.getPath().getName();
            if (filename.startsWith("part-")) {
                cacheFiles.add(status.getPath().toUri());
            }
        }

        logMessage("Start final aggregation job");

        // 最终合并Job（替代原来的多Job并行逻辑）
        Configuration finalConf = new Configuration();
        finalConf.set("circulatingStock", circulatingStock);

        Job finalJob = Job.getInstance(finalConf, "Final Aggregation Job");
        finalJob.setJarByClass(MultiStageJob.class);

        // 读取预处理输出(timeWindow, record)
        FileInputFormat.addInputPath(finalJob, new Path(intermediateTradeOutputBasePath));
        FileOutputFormat.setOutputPath(finalJob, new Path(finalOutputPath));

        // Mapper可以直接使用一个简单的Mapper输出，Reducer中进行合并逻辑
        finalJob.setMapperClass(FinalAggregationMapper.class);
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);

        finalJob.setReducerClass(FinalAggregationReducer.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);

        // 添加缓存文件
        for (URI uri : cacheFiles) {
            finalJob.addCacheFile(uri);
        }

        if (!finalJob.waitForCompletion(true)) {
            logMessage("Final Aggregation Job failed");
            System.exit(1);
        }

        logMessage("All windows processed successfully.");
        logWriter.close();
        System.exit(0);

    }
}