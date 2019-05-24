/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import twitter4j.Trend;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author alexandros
 */
public class find_top_10 {

    private Twitter init_twitter(){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setJSONStoreEnabled(true)
                .setOAuthConsumerKey("JWTfL3K0m10LNgDlrtNPKuzGd")
                .setOAuthConsumerSecret("mgVERh0AP7T6ewskEMYkdUWcZvgJZNrtnNMYzK0q6qEGQTQc2C")
                .setOAuthAccessToken("2173965894-LoPemGHsxv2w4UXlBTYkM9SiMbMlsYMsL3lYRWF")
                .setOAuthAccessTokenSecret("Aoh9orWWYmneNz16rm91XskLOoWaVDbBZZ7TQzwxE3aRk");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }
    public find_top_10() throws UnknownHostException, TwitterException {
        
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("trends_db");
        DBCollection top10_collection = db.getCollection("top10");

        
        Twitter twitter = init_twitter();

        Trend[] trends = twitter.trends().getPlaceTrends(1).getTrends();

        for (Trend trend : trends) {
            BasicDBObject dbTrend = new BasicDBObject()
                    .append("$set", new BasicDBObject("name", trend.getName())
                            .append("url", trend.getURL())
                            .append("query", trend.getQuery())
                            .append("start_time", new java.util.Date())
                    );

            System.out.println(dbTrend);
            String[] anArray = {trend.getName()};

            //if record exists update its time, else create new record
            BasicDBObject findObject = new BasicDBObject("name", new BasicDBObject("$in", anArray));
            top10_collection.update(findObject, dbTrend, true, true);

        }
        System.out.println("mpika kai telos");
    }
}
