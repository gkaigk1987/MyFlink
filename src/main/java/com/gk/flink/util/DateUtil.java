package com.gk.flink.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class DateUtil {
	
	private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

	/**
	 * 时间格式化
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String dateFormat(Date date, String pattern) {
		if(date == null) {
			return "";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.format(date);
	}
	
	/**
	 * 计算两个时间的相差的天数，后面一个减前面一个日期
	 * @param date1
	 * @param date2
	 * @return
	 * @throws ParseException 
	 */
	public static int compareForDay(Date date1, Date date2)  {
		if(null == date1 || null == date2) {
			return -1;
		}
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			date1 = sdf.parse(sdf.format(date1));
			date2 = sdf.parse(sdf.format(date2));
			cal.setTime(date1);
			long time1 = cal.getTimeInMillis();
			
			cal.setTime(date2);
			long time2 = cal.getTimeInMillis();
			
			long between_days = (time2 - time1) / (1000 * 3600 * 24);
			return Integer.parseInt(String.valueOf(between_days));
		} catch (ParseException e) {
			logger.error("日期格式化出错！");
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return -1;
	}
	
	/**
	 * 日期比较
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static int compare(Date beginDate, Date endDate) {
		int ret = 1;
		long beginTime = beginDate.getTime();
		long endTime = endDate.getTime();

		if (beginTime > endTime) {
			ret = 2;
		}
		if (beginTime == endTime) {
			ret = 1;
		}
		if (beginTime < endTime) {
			ret = 0;
		}
		return ret;
	}
	
	/**
	 * 返回较大类型的日期的字符串
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static String dateCompare(Date d1,Date d2) {
		if(null == d1) {
			if(null == d2) {
				return "";
			}else {
				return dateFormat(d2,"yyyy-MM-dd HH:mm:ss");
			}
		}else {
			if(null == d2) {
				return dateFormat(d1,"yyyy-MM-dd HH:mm:ss");
			}else {
				int i = compare(d1,d2);
				if(2 == i) {
					return dateFormat(d1,"yyyy-MM-dd HH:mm:ss");
				}else if(1 == i) {
					return dateFormat(d1,"yyyy-MM-dd HH:mm:ss");
				}else {
					return dateFormat(d2,"yyyy-MM-dd HH:mm:ss");
				}
			}
		}
		
	}
	
	/**
	 * 返回较大类型的日期
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static Date dateCompareForDate(Date d1,Date d2) {
		if(null == d1) {
			if(null == d2) {
				return null;
			}else {
				return d2;
			}
		}else {
			if(null == d2) {
				return d1;
			}else {
				int i = compare(d1,d2);
				if(2 == i) {
					return d1;
				}else if(1 == i) {
					return d1;
				}else {
					return d2;
				}
			}
		}
	}
	
	/**
	 * 字符串传日期
	 * @param dateStr
	 * @return
	 */
	public static Date string2Date(String dateStr,String pattern) {
		if(StringUtils.isEmpty(dateStr)) {
			return null;
		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		try {
			return simpleDateFormat.parse(dateStr);
		} catch (ParseException e) {
			logger.error("字符串转日期出错",e);
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 日期类型的字符串转long
	 * @param dateStr
	 * @param pattern
	 * @return
	 */
	public static Long string2Long(String dateStr,String pattern) {
		Date date = string2Date(dateStr,pattern);
		if(null == date) {
			return null;
		}
		return date.getTime()/1000;
	}
	
	/**
	 * 日期long型转日期字符串
	 * @param dateLong
	 * @param pattern
	 * @return
	 */
	public static String long2String(long dateLong,String pattern) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		Date date = new Date(dateLong * 1000);
		return simpleDateFormat.format(date);
	}
	
	/**
	 * 获取两个日期间的月份
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static List<String> getMonthRange(String startTime,String endTime) {
		if(StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime)) {
			return Lists.newArrayList(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM")));
		}else if(StringUtils.isEmpty(startTime)) {
			if(endTime.length() == 7) {
				//只到月份
				endTime += "-01";
			}
			return Lists.newArrayList(LocalDate.parse(endTime).format(DateTimeFormatter.ofPattern("yyyy-MM")));
		}else if(StringUtils.isEmpty(endTime)) {
			if(startTime.length() == 7) {
				//只到月份
				startTime += "-01";
			}
			return Lists.newArrayList(LocalDate.parse(startTime).format(DateTimeFormatter.ofPattern("yyyy-MM")));
		}else {
			List<String> result = new ArrayList<>();
			if(startTime.length() == 7) {
				//只到月份
				startTime += "-01";
			}
			if(endTime.length() == 7) {
				//只到月份
				endTime += "-01";
			}
			LocalDate start = LocalDate.parse(startTime,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			LocalDate end = LocalDate.parse(endTime,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			LocalDate ld = LocalDate.from(start);
			while(!ld.isAfter(end)) {
				result.add(ld.format(DateTimeFormatter.ofPattern("yyyy-MM")));
				ld = ld.plus(1, ChronoUnit.MONTHS);
			}
//			result.add(end.format(DateTimeFormatter.ofPattern("yyyy-MM")));
			return result;
		}
	}
	
	/**
	 * 获取两个日期间所有的日期
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static List<String> getDayRange(String startTime,String endTime) {
		if(StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime)) {
			return Lists.newArrayList(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		}else if(StringUtils.isEmpty(startTime)) {
			return Lists.newArrayList(LocalDate.parse(endTime).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		}else if(StringUtils.isEmpty(endTime)) {
			return Lists.newArrayList(LocalDate.parse(startTime).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		}else {
			List<String> result = new ArrayList<>();
			LocalDate start = LocalDate.parse(startTime,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			LocalDate end = LocalDate.parse(endTime,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			LocalDate ld = LocalDate.from(start);
			while(!ld.isAfter(end)) {
				result.add(ld.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
				ld = ld.plus(1, ChronoUnit.DAYS);
			}
//			result.add(end.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
			return result;
		}
	}
	
	/**
	 * 获取日期的月份
	 * @param date
	 * @return
	 */
	public static String getDateMonth(String date) {
		if(StringUtils.isEmpty(date)) {
			return null;
		}
		return LocalDate.parse(date).format(DateTimeFormatter.ofPattern("yyyy-MM"));
	}
	
	/**
	 * 获取某月的第一天
	 * @param dayTime
	 * @return
	 */
	public static String getFirstDayOfMonth(String dayTime) {
		if(dayTime.length() == 7) {
			//只到月份
			dayTime += "-01";
		}
		LocalDate firstDayOfMonth = LocalDate.parse(dayTime).with(TemporalAdjusters.firstDayOfMonth());
		return firstDayOfMonth.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	}
	
	/**
	 * 获取某月的最后一天
	 * @param dayTime
	 * @return
	 */
	public static String getLastDayOfMonth(String dayTime) {
		if(dayTime.length() == 7) {
			//只到月份
			dayTime += "-01";
		}
		LocalDate lastDayOfMonth = LocalDate.parse(dayTime).with(TemporalAdjusters.lastDayOfMonth());
		return lastDayOfMonth.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	}
	
	/**
	 * 当日期类型只到月份时获取该月份的第一天
	 * @param month
	 * @return
	 */
	public static String formatMonthFirstDate(String month) {
		if(StringUtils.isNotEmpty(month) && month.length() == 7) {
			return getFirstDayOfMonth(month);
		}
		return month;
	}
	
	/**
	 * 当日期类型只到月份时获取该月份的最后一天
	 * @param month
	 * @return
	 */
	public static String formatMonthLastDate(String month) {
		if(StringUtils.isNotEmpty(month) && month.length() == 7) {
			return getLastDayOfMonth(month);
		}
		return month;
	}
	
	/**
	 * 字符串日期比较
	 * date1大于等于date2返回true
	 * 否则返回false
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static boolean dateCompare(String date1,String date2) {
		Date d1 = string2Date(date1,"yyyy-MM-dd");
		Date d2 = string2Date(date2,"yyyy-MM-dd");
		int i = compare(d1, d2);
		if(i >= 1 ) {
			//date1 大于等于 date2
			return true;
		}else {
			//date1 小于 date2
			return false;
		}
	}
	
	/**
	 * 获取当前年度
	 * @return
	 */
	public static Integer getCurrentYear() {
		Calendar instance = Calendar.getInstance();
		return instance.get(Calendar.YEAR);
	}
	
	/**
	 * 日期转Long
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static Long date2Long(Date date, String pattern) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			String format = sdf.format(date);
			return sdf.parse(format).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 字符串日期比较
	 * date1大于等于date2返回1
	 * 否则返回0
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int dateCompareToInt(String date1,String date2) {
		Date d1 = string2Date(date1,"yyyy-MM");
		Date d2 = string2Date(date2,"yyyy-MM");
		int i = compare(d1, d2);
		if(i >= 1 ) {
			return -1;
		}else {
			return 1;
		}
	}
	
	/**
	 * 获取指定日期前n天的日期
	 * @param date:格式yyyy-MM-dd
	 * @param days
	 * @return
	 */
	public static String getDateByMinusDays(String date,int days) {
		LocalDate localDate = LocalDate.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		LocalDate minusDate = localDate.minusDays(days);
		String reDate = minusDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return reDate;
	}
	
	/**
	 * 获取指定日期前n天的日期
	 * @param date
	 * @param days
	 * @return
	 */
	public static String getDateByMinusDays(Date date,int days) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return getDateByMinusDays(dateFormat.format(date),days);
	}
	
	/**
	 * 获取指定日期后n天的日期
	 * @param date:格式yyyy-MM-dd
	 * @param days
	 * @return
	 */
	public static String getDateByPlusDays(String date,int days) {
		LocalDate localDate = LocalDate.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		LocalDate plusDate = localDate.plusDays(days);
		String reDate = plusDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return reDate;
	}
	
	/**
	 * 获取指定日期后n天的日期
	 * @param date
	 * @param days
	 * @return
	 */
	public static String getDateByPlusDays(Date date,int days) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return getDateByPlusDays(dateFormat.format(date),days);
	}
	
	/**
	 * 获取指定日期后n年的日期
	 * @param date
	 * @param year
	 * @return
	 * @author gk
	 */
	public static String getDateByPlusYears(String date,int year) {
		LocalDate localDate = LocalDate.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		LocalDate plusDate = localDate.plusYears(year);
		String reDate = plusDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return reDate;
	}
	
	/**
	 * 获取指定日期后n年的日期
	 * @param date
	 * @param year
	 * @return
	 * @author gk
	 */
	public static String getDateByPlusYears(Date date,int year) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return getDateByPlusYears(dateFormat.format(date),year);
	}
	
	/**
	 * 日期与当前系统日期比较,如果传入的日期在当前系统日期之后</br>
	 * 或等于当前日期，则返回true,否则返款false
	 * @param date
	 * @return
	 * @author gk
	 */
	public static boolean compareDateNow(String date) {
		LocalDate now = LocalDate.now();
		LocalDate localDate = LocalDate.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return localDate.isEqual(now) || localDate.isAfter(now);
	}
}
