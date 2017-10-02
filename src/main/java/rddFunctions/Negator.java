/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rddFunctions;

import java.net.InetAddress;
import java.util.*;
import java.util.Iterator;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 *
 * @author tamvadss
 */
public class Negator   implements  PairFlatMapFunction< Iterator<Tuple2<Integer,Integer >>,Integer,String> {

     
    public Iterable<Tuple2<Integer, String>> call(Iterator<Tuple2<Integer, Integer>> iterator) throws Exception {
                
        List <Tuple2<Integer, String>> result= new ArrayList <Tuple2<Integer, String>>();
        
        while (iterator.hasNext()) {
            Tuple2<Integer, Integer> tuple = iterator.next();
            int partitionNumber = tuple._1;
            int value = tuple._2;
            
            Tuple2<Integer, String> outputTuple = new Tuple2<Integer, String> (partitionNumber, " IP ADDRESS and value "+ InetAddress.getLocalHost().getHostName() +(-value) ) ;
            result.add(outputTuple); 
        }
        
        return result;
    }
    
}
