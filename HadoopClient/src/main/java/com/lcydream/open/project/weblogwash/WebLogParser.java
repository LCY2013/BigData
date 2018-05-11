package com.lcydream.open.project.weblogwash;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebLogParser {

	static SimpleDateFormat  sd1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);

	static SimpleDateFormat sd2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();
		String[] arr = line.split(" ");
		if (arr.length > 11) {
			webLogBean.setRemote_addr(arr[0]);
			webLogBean.setRemote_user(arr[1]);
			webLogBean.setTime_local(parseTime(arr[3].substring(1)));
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBody_bytes_sent(arr[9]);
			webLogBean.setHttp_referer(arr[10]);

			if (arr.length > 12) {
				webLogBean.setHttp_user_agent(arr[11] + " " + arr[12]);
			} else {
				webLogBean.setHttp_user_agent(arr[11]);
			}
			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {// 大于400，HTTP错误
				webLogBean.setValid(false);
			}
		} else {
			webLogBean.setValid(false);
		}
		return webLogBean;
	}

	public static String parseTime(String dt) {

		String timeString = "";
		try {
			Date parse = sd1.parse(dt);
			timeString = sd2.format(parse);

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return timeString;
	}

	static Pattern p = Pattern.compile(".*(.js|.png|.css|.jpg)");
	/**
	 * 过滤掉静态资源
	 * @param webLogBean
	 */
	public static boolean filterStaticResource(WebLogBean webLogBean){

		if(webLogBean.getRequest()==null
				||"".equals(webLogBean.getRequest().trim())){
			return false;
		}else {
			Matcher m=p.matcher(webLogBean.getRequest());
			if(m.find()){
				return false;
			}
			return true;
		}
	}

	public static void main(String[] args) {
		//WebLogParser wp = new WebLogParser();
		String parseTime = WebLogParser.parseTime("18/Sep/2013:06:49:48");
		System.out.println(parseTime);
	}

}
