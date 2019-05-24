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
import static ergasia_twitter.Ergasia_twitter.trendsNames;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author alexandros
 */
public class stream_users_to_search {
    

    
    private TwitterStream twitterStream;
    private long[] user_ids;
    
    private boolean user_id_exists(long id){
        for (long user_id : user_ids) {
            if(user_id==id)
                return true;
        }
        return false;
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
    private void insert_tweet_in_db(String JSONStatus,long user_id,DBCollection tweets_from_users_collection){
        //twitter or twitter4j bug? sends user ids we not searching
        if(!user_id_exists(user_id))
            return;
        BasicDBObject json_status = (BasicDBObject) JSON.parse(JSONStatus);
        json_status.remove("user");
        json_status.append("user_id", user_id);
        
        
        tweets_from_users_collection.insert(json_status);
        System.out.println(json_status);
    }
   
    public void kill_thread(){
        twitterStream.clearListeners();
        twitterStream.cleanUp();
        twitterStream.shutdown();

    }
    public stream_users_to_search() throws UnknownHostException, TwitterException{
        
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB( "trends_db" );
        DBCollection users_to_search_collection = db.getCollection("users_to_search");
        DBCollection tweets_from_users_collection = db.getCollection("tweets_from_users");
        
        //find user ids
        DBCursor find = users_to_search_collection.find(new BasicDBObject(), new BasicDBObject().append("user_id", "1"));
        user_ids = find.toArray().stream().mapToLong(sc -> Long.valueOf(sc.get("user_id").toString())).toArray();
        
        
        
        
        
        StatusListener listener = new StatusListener(){
        @Override
        public void onStatus(Status status) {
            
            long user_id = status.getUser().getId();
            
            String rawJSON = DataObjectFactory.getRawJSON(status);
            insert_tweet_in_db(rawJSON, user_id, tweets_from_users_collection);
            
            
            
             

            
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
    
    //create filter with user ids
    FilterQuery track = new FilterQuery().follow(user_ids);
    //begin to watch
    twitterStream.filter( track );
    
    }
}
