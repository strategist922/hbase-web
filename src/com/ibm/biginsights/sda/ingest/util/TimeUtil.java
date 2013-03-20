/**
 * 
 */
package com.ibm.biginsights.sda.ingest.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Julian zhouxbj@cn.ibm.com 
 * 
 */
public class TimeUtil {

	/**
	 * Parse time to "yyyy-MM-dd-HH-mm-ss" from unix format.
	 * 
	 * @param timeInSec - Unix time format
	 * @return
	 */
	public static String parseTimeFromUnixFmt(String timeInSec) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(Long.parseLong(timeInSec) * 1000);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		return sdf.format(c.getTime());
	}

	/**
	 * Parse time to unix format.
	 * 
	 * @param time - Format must be "yyyy-MM-dd-HH-mm-ss"
	 * @return
	 */
	public static String parseTimeToUnixFmt(String time) {

		String[] parts = time.split("-");

		if (parts.length >= 3) {
			int years = Integer.parseInt(parts[0]);
			int months = Integer.parseInt(parts[1]) - 1;
			int days = Integer.parseInt(parts[2]);
			int hours = Integer.parseInt(parts[3]);
			int minutes = Integer.parseInt(parts[4]);
			int seconds = Integer.parseInt(parts[5]);

			GregorianCalendar gc = new GregorianCalendar(years, months, days,
					hours, minutes, seconds);

			return Long.toString(gc.getTimeInMillis() / 1000);
		}

		return "";
	}

	public static void main(String[] args) {
		System.out.println(TimeUtil.parseTimeFromUnixFmt("1351819073"));
		System.out.println(TimeUtil.parseTimeToUnixFmt("2012-10-01-00-00-00"));
	}
}
