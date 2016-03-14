package main;

import config.TwitterConfiguration;
import io.TwitterFileWriter;
import io.TwitterScanner;

import javax.swing.plaf.synth.SynthTextAreaUI;

/**
 * Created by daniel on 1/22/16.
 */

/*
http://twitter4j.org/en/code-examples.html
http://keyurj.blogspot.com/2014/02/reading-twitter-stream-using-twitter4j.html
http://stackoverflow.com/questions/9395999/twitter4j-access-tweet-information-from-streaming-api
https://github.com/yusuke/twitter4j
* */

public class Main {


    public static void main(String[] args) {

        /* Form
        * Arg1: Config dir
        * Arg2: result dir
        * Arg3: Num of tweets
        * Arg4: Query term, "term1 term2 term3"
        * */

        final int tweetsInQuery = 100; // Max 100 min 15
        final String baseDir = "/Users/daniel/Documents/UMKC/Spring2016/PrinciplesOfBigData/TweetData";

        // The configuration
        String stringQuery;
        int tweetAmount;
        String configDir;
        String configDirBatch;
        String outputDir;
        String date = "";

        configDir = baseDir + "/config.txt";
        configDirBatch = baseDir + "/config2.txt";
        outputDir = baseDir + "/test-election2016-batch-2.json";
        tweetAmount = 100000;
        stringQuery = "election2016";

        date = (args.length > 4) ? args[4] : null;


//        configDir = args[0];
//        outputDir = args[1];
//        tweetAmount = Integer.parseInt(args[2]);
//        stringQuery = args[3];


        // Count the tweets, lets see how much it takes to reach 1K
        try {
            TwitterFileWriter writer = new TwitterFileWriter(outputDir);
            TwitterConfiguration myConfig = new TwitterConfiguration(configDir);

            TwitterScanner scanner = new TwitterScanner(stringQuery,
                    tweetsInQuery,
                    tweetAmount,
                    1000,
                    myConfig.getConfiguration(),
                    writer
            );

            if(date != null)
                scanner.setTweetDate(date);

            scanner.computeBatch(configDirBatch);
        }
        catch (Exception twE){
            twE.printStackTrace();
            System.out.println("Message " + twE.getMessage());
        }

        System.out.println("\n ## End of program \n");
    }



}
