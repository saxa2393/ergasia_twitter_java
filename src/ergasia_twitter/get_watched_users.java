/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;

/**
 *
 * @author alexandros
 */
public class get_watched_users {
    public DBCursor get_watched_users() throws UnknownHostException{
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("trends_db");
        DBCollection users_to_search = db.getCollection("users_to_search");
        return users_to_search.find();
    }
}
