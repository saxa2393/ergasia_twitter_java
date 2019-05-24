/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import static ergasia_twitter.Ergasia_twitter.check;
import static ergasia_twitter.Ergasia_twitter.trendsNames;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Trend;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author alexandros
 */
public class stream_trend_tweets {
    
    private TwitterStream twitterStream;
    private static  Map<String, String> check(String x){
            Map<String, String> map = new HashMap<String, String>();
            for (String trendsName : trendsNames) {
                if(x.toLowerCase().contains(trendsName.toLowerCase())){
                    map.put("ok", "true");
                    map.put("trending_name",trendsName);
            
                }
                        
            }
            return map;
    }
    private TwitterStream init_twitter(){
        ConfigurationBuilder cba = new ConfigurationBuilder();
        cba.setDebugEnabled(true)
          .setJSONStoreEnabled(true)
          .setOAuthConsumerKey("JWTfL3K0m10LNgDlrtNPKuzGd")
          .setOAuthConsumerSecret("mgVERh0AP7T6ewskEMYkdUWcZvgJZNrtnNMYzK0q6qEGQTQc2C")
          .setOAuthAccessToken("2173965894-LoPemGHsxv2w4UXlBTYkM9SiMbMlsYMsL3lYRWF")
          .setOAuthAccessTokenSecret("Aoh9orWWYmneNz16rm91XskLOoWaVDbBZZ7TQzwxE3aRk");
     
        TwitterStream twitterStream = new TwitterStreamFactory(cba.build()).getInstance();
        return twitterStream;
    }
    private void insert_tweet_in_db(String JSONStatus,String trending_name,DBCollection trending_tweets_collection){
        BasicDBObject dbObject = (BasicDBObject) JSON.parse(JSONStatus);
        dbObject.append("trend_name", trending_name);

        trending_tweets_collection.insert(dbObject);
    }
    private String[] get_current_trends_to_search(DBCollection top10_collection){
       
        List<String> list =new ArrayList<String>();
        Date date = new java.util.Date();
        long ONE_MINUTE_IN_MILLIS=60000;//millisecs
        long t=date.getTime();
        //get tweets that are active the past 2 hours
        Date beforeTwoHours=new Date(t - (120 * ONE_MINUTE_IN_MILLIS));  
        BasicDBObject twoHoursQuery = new BasicDBObject("start_time",new BasicDBObject("$gt",beforeTwoHours));
        BasicDBObject returnOnlyNames = new BasicDBObject("name","1").append("_id", 0).append("start_time","1");
        //select name,start from trends where trend_time < 2hours
        DBCursor cursor = top10_collection.find(twoHoursQuery ,returnOnlyNames);

        try {
            while(cursor.hasNext()) {

                String toString = cursor.next().get("name").toString();
                list.add(toString);
                
            }
         } finally {
            cursor.close();
         }
        trendsNames = new String[list.size()];
        trendsNames = list.toArray(trendsNames);
        return trendsNames;
    }
    public void kill_thread(){
        twitterStream.clearListeners();
        twitterStream.cleanUp();
        twitterStream.shutdown();

    }
    public stream_trend_tweets() throws UnknownHostException, TwitterException{
        
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB( "trends_db" );
        DBCollection trending_tweets_collection = db.getCollection("trending_tweets");
        DBCollection top10_collection = db.getCollection("top10");
        
        
        
        
        
        
        StatusListener listener = new StatusListener(){
        @Override
        public void onStatus(Status status) {
            
            
            
            Map<String, String> checkResult = check(status.getText());
            if(!checkResult.isEmpty() && checkResult.get("ok").equals("true")){
                String rawJSON = DataObjectFactory.getRawJSON(status);
                insert_tweet_in_db(rawJSON,checkResult.get("trending_name"),trending_tweets_collection);
                System.out.println(rawJSON);
            }
            
             

            
        }
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
        public void onException(Exception ex) {
            ex.printStackTrace();
        }

            @Override
            public void onScrubGeo(long l, long l1) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
    };
    
    twitterStream = init_twitter();
    twitterStream.addListener(listener);
        
    FilterQuery track = new FilterQuery().track(get_current_trends_to_search(top10_collection));
    twitterStream.filter( track );
    
    }
}
