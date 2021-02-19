import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Imdb {

    // Read from IMDB_TITLES.txt file and use 3 different type and 3 different genre as a filter to select movie
    public static class ReadTitlesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split(";");
            String id = lineValues[0];
            String type = lineValues[1].toString();
            String name = lineValues[2];
            String year = lineValues[3];
            String temp = lineValues[4];
            line = lineValues[4].toString();
            String[] genreList = line.split(",");
            ArrayList<String> genre = new ArrayList<String>();
            for (int i=0;i<genreList.length;i++) {
                genre.add(genreList[i]);
            }
            String tag = type + ";" + name + ";" + year + ";" + temp;
            
            if ((type.equals("movie") || type.equals("tvSeries") || type.equals("tvMovie")) && (genre.contains("Comedy") || genre.contains("Romance") || genre.contains("Action")))
                context.write(new Text(id), new Text("title    " + tag));
        }
    }

    // Read from IMDB_ACTORS.txt file
    public static class ReadActorsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split(";");
            String id = lineValues[0];
            String actorId = lineValues[1];
            String actorName = lineValues[2];
            String tag = actorId + ";" + actorName;
            context.write(new Text(id), new Text("actor    " + tag));
        }
    }

    // Read from IMDB_DIRECTORS.txt file
    public static class ReadDirectorsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split(";");
            String id = lineValues[0];
            String directorId = lineValues[1];
            context.write(new Text(id), new Text("director    " + directorId));
        }
    }

    // check if there exists any actor and director combination for 3 title type, If it exists, write each combination as a new line to a file
    public static class ActorDirectorReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce ( Text key, Iterable<Text> values, Context context )
                           throws IOException, InterruptedException {
            String id = "";
            String name = "";
            String year = "";
            String type = "";
            String genre = "";
            ArrayList<String> actorIds = new ArrayList<String>();
            ArrayList<String> actorNames = new ArrayList<String>();
            ArrayList<String> directorIds = new ArrayList<String>();
            String line = "";
            int count = 0;
            boolean present = false;
            for (Text t: values) {
                line = t.toString();
                String[] parts = line.split("    ");
                if (parts[0].equals("title")) {
                    line = parts[1].toString();
                    String[] lineValues = line.split(";");
                    type = lineValues[0];
                    name = lineValues[1];
                    if (lineValues.length > 2) {
                        year = lineValues[2];
                        if (lineValues.length > 3)
                            genre = lineValues[3];
                    }
                    // count += 1;
                    present = true;
                }
                else if (parts[0].equals("actor")) {
                    line = parts[1].toString();
                    String[] lineValues = line.split(";");
                    actorIds.add(lineValues[0]);
                    actorNames.add(lineValues[1]);
                    // count += 0;
                }
                else if (parts[0].equals("director")) {
                    line = parts[1].toString();
                    directorIds.add(line);
                    // count += 0;
                }
            }
            if (present) {
                if (actorIds.size() > 0 && directorIds.size() > 0) {
                    // int temp = 0;
                    for (int i = 0;i < directorIds.size();i++) {
                        if (actorIds.contains(directorIds.get(i))) {
                            // temp += 1;
                            int index = actorIds.indexOf(directorIds.get(i));
                            String actorName = actorNames.get(index);
                            String result = "    " + name + "    " + actorName + "    " + year + "    " + genre;
                            context.write(new Text(type),new Text(result));
                            // temp += 1;
                        }
                    }
                }
            }
        }
    }

    // Take input from output of reducer 2, Emit title type as key and 1 as value
    public static class TitleTypeMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split("    ");
            context.write(new Text(lineValues[0]),new LongWritable(1));
        }
    }

    // Count the values received and output the sum as value
    public static class CountTitleTypeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce ( Text key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v: values) {
                count++;
            };
            context.write(key, new LongWritable(count));
        }
    }

    // select genre of movie which are released on or after year 2000
    public static class MovieGenreMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split(";");
            String id = lineValues[0];
            String type = lineValues[1].toString();
            String name = lineValues[2];
            String year = lineValues[3];
            if (type.equals("movie")) {
                try {
                    long l = Long.parseLong(year);
                    if (l >= 2000) {
                        String temp = lineValues[4];
                        line = lineValues[4].toString();
                        String[] genreList = line.split(",");
                        for (int i=0;i<genreList.length;i++) {
                            String newKey = year + "," + genreList[i];
                            context.write(new Text(newKey), new LongWritable(1));
                        }
                    }
                } catch (NumberFormatException nfe) {
                
                }
            }
        }
    }

    // count number of each gener type movie released for each year from 2000
    public static class GenreCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce ( Text key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v: values) {
                count++;
            };
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {

        // read from 3 files and select movies where actor and director are same
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "actor director");
        job.setJarByClass(Imdb.class);
        job.setNumReduceTasks(3);
        job.setReducerClass(ActorDirectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,ReadTitlesMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,ReadActorsMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,ReadDirectorsMapper.class);
        // conf.set("mapreduce.job.maps", "4");
        FileOutputFormat.setOutputPath(job, new Path(args[3]+"inter"));
        job.waitForCompletion(true);

        // count the number of times where actor and director for each title type is same
        Job jobTwo = Job.getInstance(conf, "actor director count");
        // jobTwo.setJobName("MyJob2");
        jobTwo.setJarByClass(Imdb.class);
        // jobTwo.setNumReduceTasks(3);
        jobTwo.setMapperClass(TitleTypeMapper.class);
        jobTwo.setReducerClass(CountTitleTypeReducer.class);
        jobTwo.setOutputKeyClass(Text.class);
        jobTwo.setOutputValueClass(LongWritable.class);
        jobTwo.setInputFormatClass(TextInputFormat.class);
        jobTwo.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobTwo, args[3]+"inter");
        FileOutputFormat.setOutputPath(jobTwo, new Path(args[3]));
        jobTwo.waitForCompletion(true);

        // count number of each genre type movies released from year 2000
        Job jobThree = Job.getInstance(conf, "actor director count");
        // jobTwo.setJobName("MyJob3");
        jobThree.setJarByClass(Imdb.class);
        jobThree.setMapperClass(MovieGenreMapper.class);
        jobThree.setReducerClass(GenreCountReducer.class);
        jobThree.setOutputKeyClass(Text.class);
        jobThree.setOutputValueClass(LongWritable.class);
        jobThree.setInputFormatClass(TextInputFormat.class);
        jobThree.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobThree, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobThree, new Path(args[3]+"bonus"));
        jobThree.waitForCompletion(true);
    }

}