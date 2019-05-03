package com.wdongyu.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.hive.HiveContext;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

public class SparkHive {

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

    public static long[] rowCount = {859, 1817, 2387, 13758, 26949, 140420, 785703, 22561642};

    // With ID
    public static int[] columnCount = {16, 9, 43, 36, 26, 26, 48, 18};

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

    static SparkSession spark;

    public static void main(String[] args) {

        spark = SparkSession.builder()
                .appName("SparkHive")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://172.16.1.137:31070")
                //.config("hive.metastore.uris", "thrift://hadoop-spark-master:9083")
                .enableHiveSupport()
                .getOrCreate();

        // 读取机构信息表
        // JavaRDD<Row> jgxx = notNullCheck(0);

        // 读取对公定期存款分户账
        // Dataset<Row> dfdcfz = notNullCheck(5).cache();

        // 读取对公客户信息表
        // Dataset<Row> dgkhxx = notNullCheck(6);

        // 读取总账全科目表
        // JavaRDD<Row> zzqkm = notNullCheck(7);
        //Dataset<Row> zzqkm = spark.sql("select kmbh,bz,kjrq,bszq,nbjgh from " + tableList[7]);

        // Existence Check
        // System.out.println(dfdcfz.except(zzqkm).count());

        // Statistical Verification
//        Dataset<Row> zzqkm = spark.sql("select qcjfye,qcdfye,jffse,dffse,qmjfye,qmdfye from " + tableList[7]);
//        Dataset<Row> sv = zzqkm.filter(new FilterFunction<Row>() {
//            @Override
//            public boolean call(Row row) {
//                return row.getDouble(0) - row.getDouble(1) + row.getDouble(2) - row.getDouble(3)
//                        != row.getDouble(4) - row.getDouble(5);
//            }
//        });
//        System.out.println(sv.count());

        // 账户本期月末余额=上期账户月末余额-本期借方发生额+本期贷方发生额
//        Dataset<Row> dgdcfzmx = spark.sql("select jyje,dqckzh,bz,cjrq,jyjdbz from " + tableList[3]);
//        Dataset<Row> d1 = dgdcfzmx.map(new MapFunction<Row, Row>() {
//                                    @Override
//                                    public Row call(Row row) throws Exception {
//                                        int len = row.length();
//                                        Object[] obj = new Object[len - 1];
//                                        obj[0] = row.getString(len - 1).equalsIgnoreCase("借") ? -row.getDouble(0) : row.getDouble(0);
//                                        for (int i = 1; i < len - 1; i++) {
//                                            obj[i] = row.get(i);
//                                        }
//                                        Row result = RowFactory.create(obj);
//                                        return result;
//                                    }
//                                }, RowEncoder.apply(dgdcfzmx.select("jyje","dqckzh", "bz", "cjrq").schema()))
//                                .groupBy("dqckzh", "bz", "cjrq")
//                                .sum("jyje");
//        // d1.where(d1.col("dqckzh").$eq$eq$eq("8c3cb63a3f0049e8af5c95d16c1b6c09")).show();
//        System.out.println(d1.count());


        // 对公定期存款分户账中存款余额之和
//        Dataset<Row> dgdcfz = spark.sql("select nbjgh,mxkmbh,bz,ckye from " + tableList[5]);
//        Dataset<Row> d2 = dgdcfz.groupBy("nbjgh", "mxkmbh", "bz").sum("ckye");
//        //d2.show();
//        //System.out.println(d2.count());
//
//        // 总账科目中期末贷方余额之和
//        Dataset<Row> zzqkm = spark.sql("select nbjgh,kmbh,bz,qmdfye from " + tableList[7]).cache();
//        Dataset<Row> z2 = zzqkm.groupBy("nbjgh", "kmbh", "bz").sum("qmdfye");
//        //z2.show();
//        //System.out.println(z2.count());
//
//        Dataset<Row> r = d2.join(z2).where(d2.col("nbjgh").$eq$eq$eq(z2.col("nbjgh")))
//                .where(d2.col("mxkmbh").$eq$eq$eq(z2.col("kmbh")))
//                .where(d2.col("bz").$eq$eq$eq(z2.col("bz")))
//                .filter(new FilterFunction<Row>() {
//                    @Override
//                    public boolean call(Row row) throws Exception {
//                        return !row.get(3).equals(row.get(7));
//                    }
//                });
//        // r.show();
//        System.out.println(r.count());


        // Uniqueness Check
        Dataset<Row> zzqkm = spark.sql("select kmbh,bz,kjrq,bszq,nbjgh from " + tableList[7]);
        Dataset<Row> c = zzqkm.groupBy("kmbh", "bz", "kjrq" , "bszq", "nbjgh").count()
                            .filter(new FilterFunction<Row>() {
                                @Override
                                public boolean call(Row row) throws Exception {
                                    return row.getLong(5) >= 1;
                                }
                            });
        c.show();
        //System.out.println(c.count());
        //System.out.println(zzqkm.distinct().count());

        spark.stop();
        spark.close();

    }
}
