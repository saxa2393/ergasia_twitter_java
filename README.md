A university project based on twitter's api, java and mongoDB. 
It tracks the most popular tweets, find suspicious accounts that publish only popular news and monitor their activity.
There are 2 js files that send query to mongo and extract results at json files.
The jar file extract a more complicated version using Levenstein Distance.
The 2 js files with the jar file extract the required stats.
The export_results.sh file run the above files and stores the data in a 'data' folder.

** to run jar at the terminal : java -jar "ergasia_twitter_java/dist/ergasia_twitter.jar" help
Shows menu with all the capabilities of the programm.
