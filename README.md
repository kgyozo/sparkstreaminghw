# Spark Streaming Homework

The tasks are about to process IMDB voting events and providing a "real-time" feedback about who is the best actor at the moment. In the current code, for some reason it always prints out "Chuck Norris". If you have other opinion, then it's better to fix it :)

So the homework is to finish the tasks on the StreamingExample.scala file. *Task1 is mandatory*, Task2 and Task3 are optional. (Task2 is more complex, but if you finished it then Task3 will be very easy).


# Input data
The real data was downloaded from IMDB. We are processing only movies from the TOP-250 list, and also aggregating the votes a bit. Now in each microbatch a single year of votes are processed. I also provide some small test data files to enable debugging and showing an example for the expected results. You should use the ImdbStreamingExampleTest.scala to run the streaming applications on the datasets.

In the ```actor_data``` files you have three columns separated by tabulators: actor name, movie title, movie year; while in the ```movie_rating``` files the columns are: rating (1-10), movie title, movie year. Each line in the movie rating file corresponds to 1000 similar votes in the real life. It is possible that there are different movies with the same title in different years, so you need to identify the movie by both title and year.

To make the homework easier, I generated the input files in the way that you can expect **all the votes on a movie arriving in the year of its release**. So in other words: you don't need to care now with *late arrivals* when you join the rating and the actor streams.


## Setting up the development environment

The development environment is quite straight forward, you can run the whole application from IntelliJ.

- You should clone this repo, and import the maven project into IntelliJ
- You might need to setup the scala SDK (use scala 2.11.8), maybe also install Scala plugin if it is missing
- Also you might need to mark the main/src/scala folder as "sources root" and the main/test/scala folder as "test sources root"
- Then you should be able to run the individual unit tests

Also you can use the preconfigured development environment from the Spark Core homework, described / downloadable from here: https://github.com/tewecske/sparktraininghomework




