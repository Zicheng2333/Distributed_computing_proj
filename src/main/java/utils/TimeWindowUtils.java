package utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TimeWindowUtils {
    public static List<String> generateTimeWindows(String intervalStr) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        List<String> timeWindows = new ArrayList<>();
        int interval = Integer.parseInt(intervalStr) * 1000; //转换为毫秒

        //String morningStart1 = "20190102091500000";
        //String morningEnd1 = "20190102092500001";
        //timeWindows.add(morningStart1+"-"+morningEnd1);

        String morningStart2 = "20190102093000000";
        String morningEnd2 = "20190102113000000";

        generateTimeWindowsForPeriod(sdf, morningStart2, morningEnd2, interval, timeWindows);

        String afternoonStart1 = "20190102130000000";
        String afternoonEnd1 = "20190102150000000";

        generateTimeWindowsForPeriod(sdf,afternoonStart1,afternoonEnd1,interval,timeWindows);

        //String afternoonStart2 = "20190102145700000";
        //String afternoonEnd2 = "20190102150000001"; //TODO 集合竞价阶段
        //timeWindows.add(afternoonStart2+"-"+afternoonEnd2);

        return timeWindows;
    }

    private static void generateTimeWindowsForPeriod(SimpleDateFormat sdf, String start, String end, int interval, List<String> timeWindows) throws Exception{
        Date startTime = sdf.parse(start);
        Date endTime = sdf.parse(end);

        while(startTime.before(endTime)){ //严格小于
            Date nextTime = new Date(startTime.getTime() + interval);
            if(nextTime.after(endTime)){
                timeWindows.add(sdf.format(startTime)+"-"+sdf.format(endTime));
                break;
            }
            timeWindows.add(sdf.format(startTime)+"-"+sdf.format(nextTime));
            startTime = nextTime;
        }
    }

}

