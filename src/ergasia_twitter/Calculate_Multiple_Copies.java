/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.GroupCommand;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import twitter4j.Trend;
import twitter4j.Twitter;

/**
 *
 * @author alexandros
 */
public class Calculate_Multiple_Copies {
    
    
    private int find_similar( BasicDBObject user ){
        BasicDBList tweets = (BasicDBList) user.get("tweets");
        ArrayList<Number> checked = new ArrayList();
        
        int similar = 0;
        for (int i = 0; i < tweets.size(); i++) {
            BasicDBObject tweet1 = (BasicDBObject) tweets.get(i);
            for (int j = i+1; j < tweets.size(); j++) {
                BasicDBObject tweet2 = (BasicDBObject) tweets.get(j);
                if(! checked.contains(tweet2.getDouble("id"))){
                    Number calculateDistance = new LevensteinCalc().calculateDistance(tweet1.getString("text"), tweet2.getString("text"));
                
                    if(calculateDistance.doubleValue() <= 0.10){
                       similar++;
                       checked.add(tweet2.getDouble("id"));
                    }
                }
                
                
            }
        }
       return similar;
    }
    
    private BasicDBList modify_and_return_users( Iterable<DBObject> results ){
        BasicDBList users = new BasicDBList();
        
        for (DBObject result : results) {
            BasicDBList texts = (BasicDBList) result.get("texts");
            
            BasicDBObject user = new BasicDBObject();
            BasicDBList tweets = new BasicDBList();
            user.append( "id" , (long) result.get("_id") );
            
            
            texts.removeAll(Collections.singleton(null));
            for (Iterator<Object> iterator = texts.iterator(); iterator.hasNext();) {
                BasicDBObject next = (BasicDBObject) iterator.next();
                String text = (String) next.get("text");
                
                BasicDBObject tweet = new BasicDBObject();
                tweet.append( "id" , (long) next.get("id"));
                
                BasicDBList urls =  (BasicDBList) next.get("urls");
                BasicDBList user_mentions = (BasicDBList) next.get("user_mentions");
                
                ArrayList<String> to_del = new ArrayList();
                for (Iterator<Object> iterator1 = user_mentions.iterator(); iterator1.hasNext();) {
                    
                    BasicDBList get = (BasicDBList) ((BasicDBObject) iterator1.next()).get("indices");
                    for (Iterator<Object> iterator2 = get.iterator(); iterator2.hasNext();) {
                        int begin = (int) iterator2.next();
                        int end = (int) iterator2.next();
                        to_del.add(text.substring(begin, end));
                   }
                    
                }
                for (Iterator<Object> iterator1 = urls.iterator(); iterator1.hasNext();) {
                    
                    BasicDBList get = (BasicDBList) ((BasicDBObject) iterator1.next()).get("indices");
                    for (Iterator<Object> iterator2 = get.iterator(); iterator2.hasNext();) {
                        int begin = (int) iterator2.next();
                        int end = (int) iterator2.next();
                        to_del.add(text.substring(begin, end));
                   }
                    
                }
                for (String to_del1 : to_del) {
                    text = text.replace(to_del1, "");
                }
                tweet.append( "text" , text );
                tweets.add(tweet);

                
                
                
            }
            user.append("tweets", tweets);
            users.add(user);
        }
        return users;
    }
    
    
    public int Calculate_Multiple_Copies(long user_id) throws UnknownHostException{
        
        
        
        
        
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("trends_db");
        DBCollection tweets_from_users = db.getCollection("tweets_from_users");
        BasicDBList dbl = new BasicDBList();
        BasicDBList dbl2 = new BasicDBList();
        
        
        
        String[] is_retweet = {"$retweeted_status",null};
        String[] is_reply = {"$in_reply_to_status_id",null};
        dbl2.add(new BasicDBObject("$not",new BasicDBObject("$gt",is_retweet)));
        dbl2.add(new BasicDBObject("$not",new BasicDBObject("$gt",is_reply)));
        
        
        
        dbl.add(new BasicDBObject("$and",dbl2));
        dbl.add(new BasicDBObject("text","$text")
                .append("urls", "$entities.urls")
                .append("user_mentions", "$entities.user_mentions").append("id", "$id"));
        dbl.add(null);
        
        DBObject groupFields = new BasicDBObject( "_id", "$user_id")
                .append("texts", new BasicDBObject( "$push" , new BasicDBObject("$cond",dbl)));
        DBObject group = new BasicDBObject("$group", groupFields );
        DBObject match = new BasicDBObject("$match", new BasicDBObject("user_id",user_id));

        
        AggregationOutput output = tweets_from_users.aggregate(match,group);
        BasicDBList users = modify_and_return_users(output.results());
        for (Object user : users) {
            return find_similar((BasicDBObject) user);
        }
        return 0;
        
        
            
        
        
        
        
    }
}
