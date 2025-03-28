package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bouncycastle.util.Characters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static final Logger log = LoggerFactory.getLogger(TokenizerMapper.class);
    private Text word = new Text();
    private static final Set<Character> DELIMITERS = new HashSet<Character>(
            Arrays.asList(' ', '\t', '\n', '\r', ',', '.', '!', '?', ';', ':', '-', '_', '/', '\\', '(', ')', '\'',
                    '"', '[', ']', '{', '}', '|', '@', '#', '$', '%', '^', '&', '*', '+', '=', '<', '>', '`', '~')
    );

    private static final Set<Character> START_WITH_LETTERS = new HashSet<Character>(
            Arrays.asList('a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's')
    );

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        StringBuilder currentWord = new StringBuilder();

        for (char c : line.toCharArray()) {
            if (DELIMITERS.contains(c)) {
                processWord(currentWord.toString(), context);
                currentWord.setLength(0);
            } else if (Character.isLetter(c)) {
                currentWord.append(c);
            }
        }
        // Process the last word if exists
        processWord(currentWord.toString(), context);
    }

    private void processWord(String token, Context context) throws IOException, InterruptedException {
        if (!token.isEmpty() && START_WITH_LETTERS.contains(token.charAt(0))) {
            word.set(String.valueOf(token.charAt(0)));
            context.write(word, one);
            log.info("Emitting letter: {}", token.charAt(0));
        }
    }
}
