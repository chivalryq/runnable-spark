package com.qzp.util;

import java.io.IOException;

public class _03_HBase {
    public static void main(String[] args) {
        try {
            HBaseUtil.makeHBaseConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HBaseUtil.createTable("StudentScore", new String[]{"student", "Math", "ComputerScience", "English", "Score"});

        HBaseUtil.addRecord("StudentScore", "Zhangsan",
                new String[]{"student:S_No", "student:S_Name", "student:S_Sex", "student:S_Age", "Math:C_No", "Math:C_Name", "Math:C_Credit", "English:C_No", "English:C_Name", "English:C_Credit", "Score:C_Math", "Score:C_English"},
                new String[]{"2015001", "Zhangsan", "male", "23", "123001", "Math", "2.0", "123003", "English", "3.0", "86", "69"});

        HBaseUtil.scanColumn("StudentScore", "Score:Math");

        HBaseUtil.modifyData("StudentScore", "Zhangsan", "Score:Math", "89");

//        HBaseUtil.deleteRow("StudentScore", "Zhangsan");

        HBaseUtil.scanColumn("StudentScore", "Score:Math");

        try {
            HBaseUtil.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
