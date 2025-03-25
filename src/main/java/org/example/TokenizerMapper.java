package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private static final Set<Character> START_WITH_LETTERS = new HashSet<Character>(
            Arrays.asList('a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's')
    );


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken().toLowerCase();
            if (!token.isEmpty() && START_WITH_LETTERS.contains(token.charAt(0))) {
                word.set(token.charAt(0) + "");
                context.write(word, one);
            }
        }
    }
}
