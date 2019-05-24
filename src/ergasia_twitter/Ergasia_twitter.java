/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Trend;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.api.TrendsResources;
import twitter4j.conf.ConfigurationBuilder;
import org.json.*;
import com.mongodb.*;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author alexandros
 */
public class Ergasia_twitter {

    /**
     * @param x
     * @param args the command line arguments
     */
    public static String[] trendsNames = null;
    public static  Map<String, String> check(String x){
            Map<String, String> map = new HashMap<String, String>();
            for (String trendsName : trendsNames) {
                if(x.toLowerCase().contains(trendsName.toLowerCase())){
                    map.put("ok", "true");
                    map.put("trending_name",trendsName);
            
                }
                        
            }
            return map;
    }
    public static int counter = 0;
    public static long TIME_INTERVAL = 5 * 60000; //5 mins interval
    public static stream_trend_tweets stream_trend_tweets;
    public static void main(String[] args) throws TwitterException, UnknownHostException {
        
        
        if(args.length ==0){
            System.out.println("You must enter arguments run help for more");
            System.exit(0);
        }
        if(args.length >1){
            System.out.println("You must enter only one argument run help for more");
            System.exit(0);
        }
        String comm = args[0];
        
        if(comm.compareTo("collect_trends")==0)
        {
                Timer autoUpdate = new Timer();
        
                autoUpdate.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            if(counter>0){
                                System.out.println("~~~~~~~SKOTONO~~~~~~~~~~");
                                stream_trend_tweets.kill_thread();
                                stream_trend_tweets = null;
                            }

                            System.out.println("````````KSEKINO NEA DOULEIA```````````");
                            new find_top_10();
                            stream_trend_tweets = new stream_trend_tweets();
                            counter++;
                            System.out.println("counter="+counter);
                        } catch (UnknownHostException ex) {
                            Logger.getLogger(Ergasia_twitter.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (TwitterException ex) {
                            Logger.getLogger(Ergasia_twitter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                },0,TIME_INTERVAL);
                
        }else if(comm.compareTo("get_random_users_for_watch")==0){
            new get_the_users();
        }else if(comm.compareTo("collect_tweets_from_watching_users")==0){
            new stream_users_to_search();
        }else if(comm.compareTo("generate_statistics")==0){
            BasicDBList level2_stats_list = new BasicDBList();
            BasicDBObject json_to_return = new BasicDBObject("level2_stats",level2_stats_list);
            DBCursor watched_users = new get_watched_users().get_watched_users();
            for (DBObject watched_user : watched_users) {
                long user_id = (long) watched_user.get("user_id");
                watched_user.put("statistics", 
                        new BasicDBObject("multiple_copies",new Calculate_Multiple_Copies().Calculate_Multiple_Copies(user_id))
                            .append("sources", new find_sources().find_sources(user_id)));

                level2_stats_list.add(watched_user);

            }
            System.out.println(json_to_return);
        }else if(comm.compareTo("help")==0){
            System.out.println("collect_trends : run a job to collect top 10 topics from tweeter and tweets associated with them");
            System.out.println("--------------------------");
            System.out.println("get_random_users_for_watch : run a job to find 40 users and enter them in the watching queue");
            System.out.println("--------------------------");
            System.out.println("collect_tweets_from_watching_users : run a job to collect tweets associated with current watching users");
            System.out.println("--------------------------");
            System.out.println("generate_statistic : export statistics (Levenstein,sources)");
            System.out.println("--------------------------");
            System.out.println("help : show this menu");
            System.out.println("--------------------------");
        }else{
            System.out.println("Not recognized run help for more");
        }
        
        
        
        
        }
    
}
