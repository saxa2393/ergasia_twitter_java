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
import com.mongodb.MongoClient;
import java.net.UnknownHostException;

/**
 *
 * @author alexandros
 */
public class find_sources {
    public BasicDBObject find_sources(long user_id) throws UnknownHostException{
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("trends_db");
        DBCollection tweets_from_users = db.getCollection("tweets_from_users");
        
        
        
       BasicDBObject group1 = new BasicDBObject("$group",new BasicDBObject("_id",
               new BasicDBObject("user_id","$user_id")
                                .append("source", "$source"))
               .append("count", new BasicDBObject("$sum",1)));
       BasicDBObject group2 = new BasicDBObject("$group",new BasicDBObject("_id","$_id.user_id")
               .append("source", new BasicDBObject("$push",new BasicDBObject("source","$_id.source").append("count", "$count")))
                .append("total", new BasicDBObject("$sum", "$count")));
       DBObject match = new BasicDBObject("$match", new BasicDBObject("user_id",user_id));
       AggregationOutput output = tweets_from_users.aggregate(match,group1, group2);
       Iterable<DBObject> results = output.results();
       
        for (DBObject result : results) {
            BasicDBList sources =  (BasicDBList) result.get("source");
            double max = -1;
            String max_source = null;
            for (Object source : sources) {
                BasicDBObject s = (BasicDBObject) source;
                
                double count = (double) s.getInt("count") / (int) result.get("total");
                if(count > max){
                    max = count;
                    max_source =  s.getString("source");
                }
            }
            return new BasicDBObject("max_source",max_source).append("max_frequency", max);
        }
        return null;
    }
}
