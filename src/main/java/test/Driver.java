/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.util.*;
import java.util.List; 
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import rddFunctions.Negator;

/**
 *
 * @author tamvadss
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        
        final int NUM_PARTITIONS = 10;
                
        //Driver for distributing the CPLEX  BnB solver on Spark
        SparkConf conf = new SparkConf().setAppName("SparcPlex CCA V1.4");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        List<Tuple2<Integer, Integer>> initialListCCA = new ArrayList<Tuple2<Integer, Integer>> () ;
        for (int index = 0; index < NUM_PARTITIONS; index ++) {
            initialListCCA.add( new Tuple2<Integer, Integer> (index, index  ));
        }
        
        JavaPairRDD < Integer, Integer > frontierCCA ; 
        frontierCCA = JavaPairRDD. fromJavaRDD(sc.parallelize(initialListCCA) )                 
                /*realize the movement to desired partition*/
                .partitionBy(new HashPartitioner( NUM_PARTITIONS)) 
                
                //Frontier is used many times, so cache it.
                .cache();
    
    
        //on each partition, multiply the value by -1
        List<Tuple2<Integer, String>> finalList  = frontierCCA.mapPartitionsToPair( new Negator(), true).collect();
        
        for (Tuple2<Integer, String> tuple : finalList ){
            System.out.println("" + tuple._1 +" , " + tuple._2) ;
        }
        
        List<Tuple2<Integer, Integer>> finalListInt  = frontierCCA.collect();
        
        for (Tuple2<Integer, Integer> tuple : finalListInt ){
            System.out.println("" + tuple._1 +" , " + tuple._2) ;
        }
    
        System.out.println("Test complete.") ;
    
    }
    
}
