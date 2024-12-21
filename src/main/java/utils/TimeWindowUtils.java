package utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TimeWindowUtils {
    public static List<String> generateTimeWindows(String intervalStr) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS"); //定义时间格式
        List<String> timeWindows = new ArrayList<>(); //初始化时间窗口列表
        int interval = Integer.parseInt(intervalStr) * 1000; //转换为毫秒

        String morningStart2 = "20190102093000000"; //定义上午开始时间
        String morningEnd2 = "20190102113000000"; //定义上午结束时间

        generateTimeWindowsForPeriod(sdf, morningStart2, morningEnd2, interval, timeWindows); //生成上午时间窗口

        String afternoonStart1 = "20190102130000000"; //定义下午开始时间
        String afternoonEnd1 = "20190102150000000"; //定义下午结束时间

        generateTimeWindowsForPeriod(sdf,afternoonStart1,afternoonEnd1,interval,timeWindows); //生成下午时间窗口

        return timeWindows; //返回时间窗口列表
    }

    private static void generateTimeWindowsForPeriod(SimpleDateFormat sdf, String start, String end, int interval, List<String> timeWindows) throws Exception{
        Date startTime = sdf.parse(start); //将字符串转换为时间
        Date endTime = sdf.parse(end); //将字符串转换为时间

        while(startTime.before(endTime)){ //严格小于
            Date nextTime = new Date(startTime.getTime() + interval); //计算下一个时间
            if(nextTime.after(endTime)){ //严格大于
                timeWindows.add(sdf.format(startTime)+" to "+sdf.format(endTime)); //将时间窗口添加到列表
                break;
            }
            timeWindows.add(sdf.format(startTime)+" to "+sdf.format(nextTime)); //将时间窗口添加到列表
            startTime = nextTime; //更新开始时间
        }
    }

}

