/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

/**
 *
 * @author alexandros
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import twitter4j.Trend;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import twitter4j.ResponseList;
import twitter4j.User;

public class get_the_users {
    private Twitter twitter;
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
    private boolean check_users(long [] ids ){
        
        try {
            ResponseList<User> lookupUsers = twitter.users().lookupUsers(ids);
            return true;
        } catch (TwitterException ex) {
            Logger.getLogger(get_the_users.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    } 
    public get_the_users() throws UnknownHostException{
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("trends_db");
        DBCollection users_collection = db.getCollection("users");
        DBCollection users_to_search_collection = db.getCollection("users_to_search");
        twitter = init_twitter();
        
        
        System.out.println("Mpika kai count = " + users_collection.count());
        BasicDBObject db_count = new BasicDBObject().append("count", -1);
        DBCursor cursor = users_collection.find().sort(db_count).limit(1);
        try {
            while(cursor.hasNext()) {

                double i = (double) cursor.next().get("count");
                double quarter = i/4;
                System.out.println(quarter);
                for (int j = 0; j < 4; j++) {
                    double down_border = quarter*j;
                    double up_border = quarter*(j+1);
                    System.out.println("low border: "+down_border);
                    System.out.println("up border: "+up_border);
                    BasicDBObject db_borders = new BasicDBObject()
                            .append("count", new BasicDBObject()
                                    .append("$lte", up_border)
                                    .append("$gte", down_border));
                    int count = users_collection.find(db_borders).count();
                    count -= 10; // to have at least 10 documents in the end for skip
                    long[] toArray;
                    do {                        
                        DBCursor limit = users_collection.find(db_borders).limit(10).skip( new Random().nextInt(count) );
                    
                        toArray = limit.toArray().stream().mapToLong(sc -> Long.valueOf(sc.get("_id").toString())).toArray();
                        for (long u : toArray) {
                            //System.out.println(u);
                            double frequency = (double) limit.toArray().stream().filter(p -> Long.valueOf(p.get("_id").toString()) == u).findFirst().get().get("count");
                            //System.out.println(frequency);
                            
                            /*users_to_search_collection.insert(new BasicDBObject()
                                    .append("user_id", u)
                                    .append("frequency", frequency)
                                    .append("quarter", j+1));*/
                        }
                        System.out.println();

                    } while (!check_users(toArray));
                                        
                }
                
                
            }
         } finally {
            cursor.close();
         }
        
    }
}
