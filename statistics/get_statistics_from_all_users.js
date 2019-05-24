var x = db.users.aggregate(
  [

  {
    $project : {
      "_id" : 0,
      "user_id" : "$user_id",
      "frequency": "$frequency",
      "followers_count" : "$user.followers_count",
      "friends_count" : "$user.friends_count",
      "followers/friends" : {$cond: { if: { $gt: [ "$user.friends_count", 0 ] }, then: { $divide: [ "$user.followers_count" , "$user.friends_count" ] }, else: "undefined" }},
      "account_age" : "$user.created_at"
    }

  }

  ],
  {
    allowDiskUse : true
  }
);
printjson(x.map(function(x){x.user_id = typeof x.user_id.toNumber == 'function' ? x.user_id.toNumber().toString() : x.user_id;x.account_age=x.account_age.toString();return x;}))
