package com.lakala.rtmml.util;

import java.io.FileOutputStream;

public class Test {
  public static void main(String[] args) throws Exception {
    //Example of usage:
    // String shanghai = "\u4E0A\u6D77";
    String shanghai = "iProcHearbeat���·�����Ϣ";
    byte[] out = UnicodeUtil.convert(shanghai.getBytes(), "GBK"); //Shanghai in Chinese
    FileOutputStream fos = new FileOutputStream("/tmp/out.htm");
    fos.write(out);
    fos.close();
  }
}
