import org.apache.spark.sql.*;

public class MoviesRatings {
    public static void main(String[] args) throws AnalysisException {

        // data paths
        String usersPath = "tmp/users.dat";
        String moviesPath = "tmp/movies.dat";
        String ratingsPath = "tmp/ratings.dat";

        // building spark session with one worker thread (locally)
        SparkSession sparkSession = SparkSession.builder().appName("MoviesRanking").master("local[1]").getOrCreate();

        // Read data from csv and also adding header to it for better understanding
        //"UserID","Gender","Age","Occupation","Zip-code"
        Dataset<Row> users = sparkSession.read().option("delimiter","::").csv(usersPath).toDF("UserID","Gender","Age","Occupation","Zip-code");

        // view the table
        //users.show();
        // count rows of table
        //users.count()

        // "UserID","MovieID","Rating","Timestamp"
        Dataset<Row> ratings = sparkSession.read().option("delimiter","::").csv(ratingsPath).toDF("UserID","MovieID","Rating","Timestamp");

        //MovieID::Title::Genres
        Dataset<Row> movies = sparkSession.read().option("delimiter","::").csv(moviesPath).toDF("MovieID","Title","Genres");


        // for the temporary view and we can also use sql functions on it.
        ratings.createGlobalTempView("ratings");
        users.createGlobalTempView("users");
        movies.createGlobalTempView("movies");

        // for getting date out of Title in movies
        String moviesTitle = "Replace(SUBSTRING_INDEX(movies.Title,'(',-1),')','')";

        // first query to fetch all data we need.
        String sqlQuery =
                "Select users.Age, ratings.Rating, "+moviesTitle+" as date," +
                "movies.Genres from global_temp.users join global_temp.ratings on users.UserID = ratings.UserID join " +
                "global_temp.movies on ratings.MovieID = movies.MovieID where "+moviesTitle+" > 1989 and users.age between 18 and 20";

        Dataset<Row> df_firstFetch = sparkSession.sql(sqlQuery);
        df_firstFetch.createGlobalTempView("first_fetch");

        // once we get all data, now we have to divide Genres column into rows so we can get each category individually.
        // Example: Genres in 1st row = A|B|C will be divide in three rows with values 1st row = A, 2nd row = B, 3rd row = C
        Dataset<Row> df_secondFetch = sparkSession.sql("Select  *,explode(split(Genres,'[|]')) as cat from global_temp.first_fetch");

        df_secondFetch.createGlobalTempView("second_fetch");


        //so now we have Genres and years individually. we just take avg rating with group by genres,year
        Dataset<Row> df_finalResult = sparkSession.sql("Select  cat, date, round(avg(Rating),1) as rating from global_temp.second_fetch group by cat,date order by date");
        //df_finalResult.show(200);

        // saving view into file
        // it will be in path output/avg_ratings with name part-something.csv
        df_finalResult.coalesce(1).write().mode(SaveMode.Overwrite).csv("output/avg_ratings");

        System.out.println("Done");

    }
    }
