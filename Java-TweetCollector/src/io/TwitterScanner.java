package io;

import config.TwitterBatchConfiguration;
import config.TwitterConfiguration;
import twitter4j.*;
import twitter4j.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by daniel on 1/25/16.
 */
public class TwitterScanner {

    private long limitSecondsToRest;
    private int limitNumOfQueries;
    private long limitMaxId;
    private int limitTweetsInMemory;

    private int _tweetAmount;
    private int _tweetsCount;
    private int _tweetsInQuery;
    private String _strQuery;
    private String tweetDate;

    private long _totalMillisecs;
    private ArrayList<Status> temporaryTweetStorage;
    private int temporaryQueriesCount;

    private Configuration _twitterConfig;

    private Twitter twitterInterface;
    private Query twitterQuery;
    private ITweetReceiver delegate;


    public TwitterScanner(String strSearchQuery,
                          int tweetsInQuery,
                          int tweetAmount,
                          int tweetsInMemory,
                          Configuration twitterConfig,
                          ITweetReceiver delegate){

        _strQuery = strSearchQuery;
        _tweetsInQuery = tweetsInQuery;
        _tweetAmount = tweetAmount;
        _twitterConfig = twitterConfig;
        this.delegate = delegate;
        limitTweetsInMemory = tweetsInMemory;

        this.tweetDate = "";

        init();
    }

    private void init(){
        resetTime();
        configureTwitterConnection();
        configureTwitterQuery();

        try {
            configureLimits();
        } catch (TwitterException e) {
            e.printStackTrace();
        }

        temporaryTweetStorage = new ArrayList<>();
    }

    private void printInfo(){
        String message = String.format(
                "## Message update \n" +
                        " >> Tweets stats (%d / %d) \n" +
                        " >> Queries stats (%d / %d) \n" +
                        " >> Elapsed time %d secs \n",
                _tweetsCount, _tweetAmount,
                temporaryQueriesCount, limitNumOfQueries,
                _totalMillisecs / 1000
        );
        System.out.println(message);
    }

    public void computeSearch(){
        temporaryQueriesCount = 0;
        long currentMillis;

        resetTime();
        resetTweetCounter();

        try {
            do {
                // The actual query and how much time it takes
                currentMillis = System.currentTimeMillis();
                QueryResult twitterResult = twitterInterface.search(twitterQuery);
                addTime(currentMillis, System.currentTimeMillis());

                // Get the result
                List<Status> tweets = twitterResult.getTweets();
                temporaryTweetStorage.addAll(tweets);
                for (Status tweet : tweets) {
                    if(limitMaxId == -1 || tweet.getId() < limitMaxId)
                        limitMaxId = tweet.getId();
                }

                // Update statistics and print some info
                twitterQuery.setMaxId(limitMaxId - 1);
                _tweetsCount += tweets.size();
                temporaryQueriesCount++;

                printInfo();

                // Release memory and write the file
                if(temporaryTweetStorage.size() >= limitTweetsInMemory) {
                    // Save info in disk
                    System.out.println(" >> Writing into disk");
                    delegate.doTweetStorage(temporaryTweetStorage);
                    delegate.flushTweetStorage();

                    temporaryTweetStorage.clear();
                }

                if(temporaryQueriesCount - 1 == limitNumOfQueries){
                    System.out.println(" ## Getting a good sleep for " +
                            (limitSecondsToRest / 1000) + " secs");

                    temporaryQueriesCount = 0;
                    Thread.sleep(limitSecondsToRest);

                    System.out.println(" ## Up and running");
                }
            }while(_tweetsCount < _tweetAmount);

            delegate.doTweetStorage(temporaryTweetStorage);
            delegate.flushTweetStorage();
            delegate.closeTweetStorage();

        } catch (TwitterException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void computeBatch(String batchConfigPath){
        List<TwitterBatchConfiguration> configList;


        resetTime();
        resetTweetCounter();
        try {
            configList = new TwitterConfiguration()
              .getConfigurations(batchConfigPath, _strQuery, _tweetsInQuery);

            // While we haven't finished with the tweets
            do{

                for(TwitterBatchConfiguration conf : configList) {

                    if (_tweetsCount > _tweetAmount) break;

                    _totalMillisecs = conf._limitMillisToWait;
                    limitNumOfQueries = conf._limitQueriesLeft;
                    temporaryQueriesCount = 0;

                    for (int i = 0; i < limitNumOfQueries; i++) {
                        QueryResult twitterResult = getQueryResult(conf);

                        // Get the result
                        List<Status> tweets = twitterResult.getTweets();
                        temporaryTweetStorage.addAll(tweets);
                        conf._twitterQuery.setMaxId(getMaxId(tweets));
                        _tweetsCount += tweets.size();
                        temporaryQueriesCount++;

                        printInfo();
                    }

                    System.out.println(" >> Writing into disk");
                    delegate.doTweetStorage(temporaryTweetStorage);
                    delegate.flushTweetStorage();
                    temporaryTweetStorage.clear();

                    System.out.println(" ## Getting a good sleep for " +
                            (limitSecondsToRest / 1000) / 2 + " secs");
                    temporaryQueriesCount = 0;
                    Thread.sleep(limitSecondsToRest / 2);
                    System.out.println(" ## Up and running");

                    System.out.println(" ## Finish with one configuration, get another");
                }

                System.out.println("Last date computed: " + tweetDate);
                System.out.printf("Finish with all the configuration, start again?");


            }while(_tweetsCount < _tweetAmount);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (TwitterException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("End of the program");
    }

    private QueryResult getQueryResult(TwitterBatchConfiguration conf) throws TwitterException {
        long currentMillis = System.currentTimeMillis();
        QueryResult twitterResult = conf._twitterInterface.search(conf._twitterQuery);
        addTime(currentMillis, System.currentTimeMillis());

        return twitterResult;
    }
    private long getMaxId(List<Status> tweets) {
        long id = -1;
        for (Status tweet : tweets) {
            if (id == -1 || tweet.getId() < id)
                id = tweet.getId();
        }
        return id - 1;
    }

    private String getDateFromId(List<Status> tweets, long id){
        String dateFromTweet = "";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        for(Status t : tweets){
            if(t.getId() == id){

                dateFromTweet = format.format(t.getCreatedAt());
            }
        }

        return dateFromTweet;

    }

    private void resetTime(){
        _totalMillisecs = 0;
    }
    private void resetTweetCounter(){
        _tweetsCount = 0;
    }
    private void configureTwitterConnection(){
        TwitterFactory factory = new TwitterFactory(_twitterConfig);
        twitterInterface = factory.getInstance();
    }
    private void configureTwitterQuery(){
        twitterQuery = new Query(_strQuery);
        twitterQuery.setCount(_tweetsInQuery);
        limitMaxId = -1;
        twitterQuery.setMaxId(limitMaxId);
    }
    private void configureLimits() throws TwitterException{
        Map<String, RateLimitStatus> rateLimitStatus = twitterInterface.getRateLimitStatus("search");
        RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");

        limitSecondsToRest = (searchTweetsRateLimit.getSecondsUntilReset() + 2) * 1000L;
        limitNumOfQueries = searchTweetsRateLimit.getRemaining() - 1;
    }

    private void addTime(long timeMillis1, long timeMillis2){
        _totalMillisecs += timeMillis2 - timeMillis1;
    }

    public String getTweetDate() {
        return tweetDate;
    }

    public void setTweetDate(String tweetDate) {
        this.tweetDate = tweetDate;
    }
}
