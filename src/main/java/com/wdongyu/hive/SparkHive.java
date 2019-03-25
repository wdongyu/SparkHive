package com.wdongyu.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.codehaus.janino.Java;
import java.util.List;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class SparkHive {

    private static Logger logger = Logger.getLogger("SparkHive");

    public static String[] tableList = {"T_GX_JGXX", "T_KJ_NBKMKJ", "T_JL_JYLS", "T_KJ_DGDCFZMX", "T_GX_GYB", "T_KJ_DGDCFZ", "T_KH_DGKHXX", "T_KJ_ZZQKM"};

    public static String[][] columnList = {
            {"yxjgdm", "nbjgh", "jrxkzh", "yxjgmc", "jglb", "yzbm", "wdh", "yyzt", "clsj", "jggzkssj", "jggzzzsj", "jgdz", "fzrxm", "fzrzw", "fzrlxdh", "cjrq", "id"},

            {"kmbh", "kmmc", "kjkmjc", "sjkmbh", "sjkmmc", "gsywdl", "gsywzl", "cjrq", "id"},

            {"hxjylsh", "zjylsh", "bcxh", "jyrq", "yxjgdm", "nbjgh", "jrxkzh", "mxkmbh", "jysj", "jzrq", "jzsj", "jyjgmc", "jyzh", "jyhm", "jyxtmc", "dfxh", "dfjgmc", "dfzh", "dfhm", "jyje", "zhye", "jyjdbz", "xzbz", "bz", "ywlx", "jylx", "jyqd", "jyjzmc", "jyjzh", "czgyh", "gylsh", "fhgyh", "zy", "zpzzl", "zpzh", "fpzzl", "fpzh", "cbmbz", "sjc", "zhbz", "kxhbz", "cjrq", "id"},

            {"hxjylsh", "zjylsh", "bcxh", "dqckzh", "khtybh", "yxjgdm", "jrxkzh", "nbjgh", "mxkmbh", "yxjgmc", "mxkmmc", "hxjyrq", "hxjysj", "bz", "zhmc", "jylx", "jyje", "khhjgh", "ywbljgh", "zhye", "dfzh", "dfhm", "dfxh", "dfxm", "jyqd", "xzbz", "bftqzqbz", "dbrxm", "dbrzjlb", "dbrzjhm", "jygyh", "sqgyh", "zy", "cbmbz", "jyjdbz", "cjrq"},

            {"gyh", "gh", "yxjgdm", "nbjgh", "zxjgdm", "jrxkzh", "yxjgmc", "gylx", "sfstgy", "khjlbz", "jbzwbz", "qxglbz", "ybglbz", "xdybz", "kgybz", "zhgybz", "sqbz", "sqfw", "gwbh", "gyyhjb", "gyqxjb", "sgrq", "gwzt", "cjrq", "id", "id_t"},

            {"dqckzh", "khtybh", "yxjgdm", "jrxkzh", "nbjgh", "mxkmbh", "mxkmmc", "yxjgmc", "bz", "tjkmbh", "zhmc", "dgdqckzhlx", "ckqx", "ll", "bzjzhbz", "khje", "ckye", "khrq", "khgyh","xhrq", "dqr", "zhzt", "scdhrq", "chlb", "cjrq", "id"},

            {"khtybh", "zzjgdm", "yxjgdm", "jrxkzh", "nbjgh", "khmc", "khywmc", "frdb", "frdbzjlb", "frdbzjhm", "cwry", "cwryzjlb", "cwryzjhm", "jbckzh", "jbzhkhxmc", "zczb", "zcdz", "lxdh", "yyzzh","yyzzyxjzrq", "jyfw", "clrq", "ssxy", "khlb", "dkzh", "gszh", "dszh", "mgskhtybh", "tysxbz", "sxed", "yyed", "ssgsbz", "xydjbh", "zczbbz", "sszbbz", "sszb", "zzc", "jzc", "nsr", "scjlxdgxny", "yzbm", "czhm", "ygrs", "xzqhdm", "khlx", "fxyjxh", "cjrq", "id"},

            {"jrxkzh", "yhjgdm", "nbjgh", "yhjgmc", "kmbh", "kmmc", "kmjc", "kmlx", "qcjfye", "qcdfye", "jffse", "dffse", "qmjfye", "qmdfye", "bz", "kjrq", "bszq", "id"}};

    // With ID
    public static int[] columnCount ={16, 9, 43, 36, 26, 26, 48, 18};

    public static String notNullClause(String op, int tableIndex) {
        int count = columnCount[tableIndex] - 1;
        String clause = columnList[tableIndex][0] + " is not null";
        for (int i = 1; i < count; i++) {
            clause += " " + op + " " + columnList[tableIndex][i] + " is not null";
        }

        return clause;
    }


    public static Dataset<Row> notNullCheck(int tableIndex) {

        String selectColumnName = "mxkmbh";

        //Dataset result = spark.sql("select " + selectColumnName +" from " + tableList[tableIndex] +
        //                                " where " + notNullClause("and", tableIndex));

        Dataset result = spark.sql("select " + selectColumnName + " from " + tableList[tableIndex]);

        System.out.println("Table " + tableList[tableIndex] + " total row: " + Long.toString(result.count()));

        return result;
    }

    static  SparkSession spark;

    public static void main(String[] args) {

        spark = SparkSession.builder()
                .appName("SparkHive")
                .enableHiveSupport()
                .getOrCreate();

        // 读取机构信息表
        // JavaRDD<Row> jgxx = notNullCheck(0);

        // 读取对公定期存款分户账
        Dataset<Row> dfdcfz = notNullCheck(5).cache();

        // 读取对公客户信息表
        // Dataset<Row> dgkhxx = notNullCheck(6);

        // 读取总账全科目表
        // JavaRDD<Row> zzqkm = notNullCheck(7);
        Dataset<Row> zzqkm = spark.sql("select kmbh from " + tableList[7]).cache();

        System.out.println(dfdcfz.except(zzqkm).count());

        spark.stop();
        spark.close();

    }
}
