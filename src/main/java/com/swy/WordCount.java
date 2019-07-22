package com.swy;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setAppName("WordCounter")//
				.setMaster("local");

		String fileName = "F:\\key.txt";

		JavaSparkContext sc = new JavaSparkContext(conf);

		/**
		 * 上述代码通过SparkConf创建JavaSparkContext，SparkConf默认去读取Spark.*的配置文件，也可以通过调用set的方法配置属性，例如上述的setMaster和setAppName。通过set方法配置的属性会覆盖读取的配置文件属性，SparkConf里面的所有set方法都支持链式调用chaining，例如上述的setMaster("local").setAppName("HelloSpark")。
		 */
		// setAppName：设置应用名字，此名字会在Spark web UI显示
		// setMaster：设置主节点URL，本例使用“local”是指本机单线程，另外还有以下选项：
		/**
		 * local[K]：本机K线程 local[*]：本机多线程，线程数与服务器核数相同
		 * spark://HOST:PORT：Spark集群地址和端口，默认端口为7077
		 * mesos://HOST:PORT：Mesos集群地址和端口，默认端口为5050 yarn：YARN集群
		 */

		// 读取文件 泛型数据类型 具体类型看 单行数据类型 参数二代表这个rdd里面有1个分区
		JavaRDD<String> lines = sc.textFile(fileName, 1);

		// 切分单词
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// 转换成键值对并计数
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// reduceByKey仅将RDD中所有K,V对中K值相同的V进行合并。
		JavaPairRDD<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer e, Integer acc) throws Exception {
				return e + acc;
			}
		}, 1);

		result.map(new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1, v1._2);
			}
		})//
				.sortBy(new Function<Tuple2<String, Integer>, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Tuple2<String, Integer> v1) throws Exception {
						return v1._2;
					}
				}, false, 1)//
				.foreach(new VoidFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> e) throws Exception {
						System.out.println("【" + e._1 + "】出现了" + e._2 + "次");
					}
				});
		sc.close();

	}
}
