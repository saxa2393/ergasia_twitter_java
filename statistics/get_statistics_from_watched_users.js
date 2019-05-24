var x = db.tweets_from_users.aggregate(
  [

  {
    $group:
    {
      _id: "$user_id",
      user_mentions_count: { $sum: {$size: "$entities.user_mentions"} },
      retweets_count: { $sum: "$retweet_count" },   //{$sum: { $cond: [ { $eq: [ "$otherField", false] } , 1, 0 ] }}
      retweets_made : { $sum : { $cond:  [ { $gt : [ "$retweeted_status" , null  ] } ,1 ,0 ] } },
      replies_made : { $sum : { $cond:  [ { $gt : [ "$in_reply_to_status_id" , null  ] } ,1 ,0 ] } },
      tweets_count : {$sum:1},
      hashtags_count: { $sum: {$size: "$entities.hashtags"} },
      avg_retweets_per_tweet : { $avg : "$retweet_count"},
      avg_hashtags_per_tweet : { $avg : {$size: "$entities.hashtags"}},
      percent_tweets_with_hashtags :  { $avg : { $cond:  [ { $gt : [ {$size: "$entities.hashtags"} , 0  ] } ,1 ,0 ] } },
      percent_tweets_with_urls :  { $avg : { $cond:  [ { $gt : [ {$size: "$entities.urls"} , 0  ] } ,1 ,0 ] } }
    }
  },
  {
    $project : {
      _id : 0,
      user_id : "$_id",
      user_mentions_count: 1,
      retweets_count: 1,
      retweets_made : 1,
      replies_made : 1,
      tweets_count : 1,
      hashtags_count: 1,
      avg_retweets_per_tweet : 1,
      avg_hashtags_per_tweet : 1,
      percent_tweets_with_hashtags : 1,
      percent_tweets_with_urls : 1,
      simple_tweets_count : { $subtract: [ '$tweets_count' , '$retweets_made']}

    }
  }

  ]
);
printjson(x.map(function(x){x.user_id=x.user_id.toNumber().toString();return x;}))
