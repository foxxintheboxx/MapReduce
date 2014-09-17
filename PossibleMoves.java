import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PossibleMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, IntWritable> {
        int boardWidth;
        int boardHeight;
        boolean OTurn;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The map function for the first mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            
            if(val.getValue() == 0){
                char c = OTurn ? 'O' : 'X';
                int area = boardWidth * boardHeight;
                String curr_board = Proj2Util.gameUnhasher(key.get(),this.boardWidth, this.boardHeight);

                for(int x = 0; x < boardWidth; x++){
                    int valid_loc = x;
                    while(valid_loc < area && curr_board.charAt(valid_loc)!=' '){
                        valid_loc += boardWidth;
                    }

                    if(valid_loc < area){
                        String new_board = curr_board.substring(0,valid_loc)+c;
                        if(valid_loc != area-1){
                            new_board = new_board+curr_board.substring(valid_loc+1);
                        }
                        context.write(new IntWritable(Proj2Util.gameHasher(new_board,boardWidth,boardHeight)),key);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        boolean lastRound;
        /**
         * Configuration and setup that occurs before reduce gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
            lastRound = context.getConfiguration().getBoolean("lastRound", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int size = 0;
            Iterator<IntWritable> v = values.iterator();
            while(v.hasNext()){
                v.next();
                size++;
            }
            //System.out.println("uykdfsghksndgfk");
            v = values.iterator();
            int[] vals = new int[size];
            int i = 0;

            while(v.hasNext()){
                vals[i] = v.next().get();
                i++;
            }

            String curr_board = Proj2Util.gameUnhasher(key.get(),boardWidth,boardHeight);
            byte b = 0;
            if(Proj2Util.gameFinished(curr_board,boardWidth,boardHeight,connectWin)){
                if(OTurn){
                    b = 2;
                } else{
                    b = 1;
                }
            } else{
                if(lastRound){
                    b = 3;
                } else{
                    b = 0;
                }
            }
            MovesWritable m = new MovesWritable(b,vals);
            context.write(key,m);
        }
    }
}
