This application reads input from the provided input paths and processed movie ratings
Application properties file (app.properties) is included in src/main/resources directory

The below versions are used for building the application
spark-3.0.1
scala-2.12.10

The below is the spark-submit command to execute the application
spark-submit --class com.vjo.movie.ratings.MovieRatings --master local --num-executors 2  --driver-memory 1g --executor-memory 2g D:\workspace\movie-ratings\target\scala-2.12\movie-ratings_2.12-0.1.jar D:\workspace\movie-ratings\src\main\resources\app.properties