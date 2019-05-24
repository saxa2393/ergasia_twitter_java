echo "Running job 1...";
mongo localhost/trends_db get_statistics_from_all_users.js --quiet > data/statistics_all_users.json;
echo "Finish job 1.";
echo "Running job 2...";
mongo localhost/trends_db get_statistics_from_watched_users.js --quiet > data/statistics_watched_users_level1.json;
echo "Finish job 2.";
echo "Running job 3...";
java -jar "../ergasia_twitter_java/dist/ergasia_twitter.jar" "generate_statistics" > data/statistics_watched_users_level2.json;
echo "Finish all!";
