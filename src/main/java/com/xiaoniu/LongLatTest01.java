package com.xiaoniu;

/**
 * date  2020/1/29-11:21
 * author zhaishuai
 * Description:
 * 运行的结果：
 */

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


public class LongLatTest01 {

    @Test
    public void main() throws UnsupportedEncodingException,
            NoSuchAlgorithmException {


        /***
            计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，该方法根据key的插入顺序排序；
            post请使用TreeMap保存<key,value>，该方法会自动将key按照字母a-z顺序排序。所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，
            但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
            以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，
            paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
         */

        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("callback", "renderReverse");
        paramsMap.put("location", "39.934,116.329");
        paramsMap.put("output", "json");
        paramsMap.put("pois", "1");
        paramsMap.put("ak", "rmFNnH8U8yRxUnG5yEY9IPysjKOGftHg");

        /**
         * 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
          */
        String paramsStr = toQueryString(paramsMap);

        /**
         * 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
         */
        System.out.println(paramsStr);

        String wholeStr = new String("/geocoder/v2/?" + paramsStr + "v7XVksGnD9MW2oKs2XmSa0WSTBY8m96B");

        /**
         * 对上面wholeStr再作utf8编码
         */
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");

        /**
         * 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
         */
        System.out.println(MD5(tempStr));
    }

    /**
     * 对Map内所有value作utf8编码，拼接返回结果
     */
    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {

        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }

        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }

        return queryString.toString();
    }

    /**
     * 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
     */
    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }


}
