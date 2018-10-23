package edu.mcw.rgd.pipelines;

import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: Dec 2, 2010
 * Time: 11:24:07 AM
 * groups methods commonly used in pipelines
 */
public class PipelineUtils {

    // safe compare strings, ignore case
    static public int safeCompareNoCase(String s1, String s2) {
        String ss1 = s1==null ? "" : s1;
        String ss2 = s2==null ? "" : s2;
        return ss1.compareToIgnoreCase(ss2);
    }

    // return current time formatted as year-month-day hour:minute:second
    static SimpleDateFormat _dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static public String getCurrentTime() {
        return _dateFormat.format(new java.util.Date());
    }

    // return formatted string showing elapsed time between two time points
    static public String formatElapsedTime(long t1, long t2) {
        assert( t1<=t2 );

        long diff = t2 - t1;

        long secondInMillis = 1000;
        long minuteInMillis = secondInMillis * 60;
        long hourInMillis = minuteInMillis * 60;
        long dayInMillis = hourInMillis * 24;
        long yearInMillis = dayInMillis * 365;

        long elapsedYears = diff / yearInMillis;
        diff = diff % yearInMillis;
        long elapsedDays = diff / dayInMillis;
        diff = diff % dayInMillis;
        long elapsedHours = diff / hourInMillis;
        diff = diff % hourInMillis;
        long elapsedMinutes = diff / minuteInMillis;
        diff = diff % minuteInMillis;
        long elapsedSeconds = diff / secondInMillis;

        StringBuilder buf = new StringBuilder(100);
        if( elapsedYears>0 ) {
            buf.append(elapsedYears).append(" years ");
        }
        if( elapsedDays>0 ) {
            buf.append(elapsedDays).append(" days ");
        }
        if( elapsedHours>0 ) {
            buf.append(elapsedHours).append(" hours ");
        }
        if( elapsedMinutes>0 ) {
            buf.append(elapsedMinutes).append(" minutes ");
        }
        if( elapsedSeconds>0 ) {
            buf.append(elapsedSeconds).append(" seconds ");
        }
        String elapsedTime = buf.toString();
        return elapsedTime.length()>0 ? elapsedTime : "0";
    }
}
