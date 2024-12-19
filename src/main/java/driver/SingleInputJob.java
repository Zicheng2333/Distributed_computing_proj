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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SingleInputJob {

    private static BufferedWriter logWriter = null;

    private static void logMessage(String msg) throws IOException {
        //此方法用于记录日记以便获得程序运行时间
        long currentTime = System.currentTimeMillis(); //初始化，获取当前时间
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//定义时间格式
        String formattedTime = sdf.format(new java.util.Date(currentTime)); //根据时间格式，将时间转换为字符串
        String logStr = "[" + formattedTime + "] " + msg; //将时间和日记信息组合
        System.out.println(logStr);
        if (logWriter != null) {
            logWriter.write(logStr);
            logWriter.newLine();
            logWriter.flush();
        } //将日记信息写入文件
    }

    public static void main(String[] args) throws Exception {
        // 初始化日志文件
        try {
            logWriter = new BufferedWriter(new FileWriter("multi_stage_timing.log", true));
        } catch (IOException e) {
            System.err.println("Failed to open log file for writing.");
            System.exit(1);
        }

        // 检查参数
        if (args.length != 5) {
            System.err.println("Usage: SingleInputJob  <tradeInputPath> <finalOutputPath> <securityID> "
                    + "<circulatingStock> <interval>"); // 提示用户正确的参数格式
            System.exit(-1); // 退出程序
        }

        // 获取参数
        String tradeInputPath = args[0]; // 获取逐笔成交表输入路径
        String finalOutputPath = args[1]; // 获取最终输出路径
        String securityID = args[2]; // 获取邮票ID
        String circulatingStock = args[3]; // 获取流通股本
        String interval = args[4]; // 获取时间窗口长度

        List<String> timeWindows = TimeWindowUtils.generateTimeWindows(interval); // 根据输入的时间窗口长度生成时间窗口

        // Step 1: 预处理交易数据
        // 将数据按照timeWindow输出 (timeWindow, record)
        StringBuilder sb = new StringBuilder();
        for (String tw : timeWindows) {
            sb.append(tw).append(",");
        } // 生成时间窗口字符串

        logMessage("Start final aggregation job");// 记录日志：开始最终聚合任务

        // 最终合并Job
        Configuration finalConf = new Configuration(); // 创建配置对象
        finalConf.set("securityID", securityID); // 设置邮票ID为先前读入的ID
        finalConf.set("timeWindows", sb.toString()); // 设置时间窗口
        finalConf.set("circulatingStock", circulatingStock); // 设置流通股本

        logMessage("Start preprocessing trade data"); // 记录日志：开始预处理交易数据

        Job finalJob = Job.getInstance(finalConf, "Final Aggregation Job"); // 创建Job对象
        finalJob.setJarByClass(SingleInputJob.class); // 设置运行的主类

        // 读取预处理输出(timeWindow, record)
        FileInputFormat.addInputPath(finalJob, new Path(tradeInputPath)); // 添加输入路径
        FileOutputFormat.setOutputPath(finalJob, new Path(finalOutputPath)); // 添加输出路径

        // Mapper可以直接使用一个简单的Mapper输出，Reducer中进行合并逻辑
        finalJob.setMapperClass(TradePreprocessingMapper.class); // 设置Mapper类
        finalJob.setMapOutputKeyClass(Text.class); // 设置Mapper输出键类型
        finalJob.setMapOutputValueClass(Text.class); // 设置Mapper输出值类型

        finalJob.setReducerClass(FinalAggregationReducer.class); // 设置Reducer类
        finalJob.setOutputKeyClass(Text.class); // 设置输出键类型
        finalJob.setOutputValueClass(Text.class); // 设置输出值类型


        if (!finalJob.waitForCompletion(true)) {
            logMessage("Final Aggregation Job failed");
            System.exit(1);
        } // 如果Job执行失败，记录日志并退出程序

        logMessage("All windows processed successfully."); // 记录日志：所有窗口成功处理
        logWriter.close(); // 关闭日志文件
        System.exit(0); // 退出程序

    }
}



