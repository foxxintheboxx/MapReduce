
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

public class SolveMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
        }

        /**
         * The map function for the second mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            int[] moves = val.getMoves();
            for(int x:moves){
                context.write(new IntWritable(x),new ByteWritable(val.getValue()));
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
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
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
            
            boolean real = false;
            byte b = 0;
            Iterator<ByteWritable> v = values.iterator();
            ArrayList<ByteWritable> bites = new ArrayList<ByteWritable>();
            while(v.hasNext()){
                ByteWritable bw = v.next();
                bites.add(bw);
                b = bw.get();
                if((b & (byte)252) == 0){
                    real = true;
                }
            }

            if(real){
                byte new_byte = best(bites);
                int[] pos_par = possibleParents(Proj2Util.gameUnhasher(key.get(),boardWidth,boardHeight));
                MovesWritable mw = new MovesWritable(new_byte,pos_par);
                context.write(key,mw); 
            }
        }

        private int[] possibleParents(String curr_board){
            char c = OTurn ? 'X' : 'O';
            int a, area = boardWidth*boardHeight;
            ArrayList<Integer> parents = new ArrayList<Integer>();

            for(int i = 0; i < boardWidth; i++){
                a = 1;
                while(a*boardWidth+i < curr_board.length()){
                    if(curr_board.charAt(a*boardWidth+i) == ' ' && 
                        curr_board.charAt((a-1)*boardWidth+i) == c){
                        String parent = curr_board.substring(0,(a-1)*boardWidth+i) + ' ';
                        if(a*boardWidth+i < curr_board.length()-1){
                            parent += curr_board.substring((a-1)*boardWidth+i+1);
                        } 
                        parents.add(new Integer(Proj2Util.gameHasher(parent,boardWidth,boardHeight)));
                    }
                    a++;
                }
            }

            int[] ret = new int[parents.size()];
            int q = 0;
            for(Integer i: parents){
                ret[q] = i.intValue();
                q++;
            }

            return ret;
        }

        /**
        * This function returns the ByteWritable the turn maker will want to choose and then
        * returns a copy of that ByteWritable with the number of moves plus one.
        */
    
        public byte best(ArrayList<ByteWritable> bites) throws IOException {

            int len = bites.size();
            System.out.println("len before: " + len);
            for (int i = 0; i < len; i++) {
                byte val = bites.get(i).get();
                byte status = (byte)(val << 6);
                status = (byte)(status >>> 6);
                if (status == 0) {
                    bites.remove(i);
                    len--;
                }
            }
         
            len = bites.size();

            for(ByteWritable b: bites){
                System.out.print(b.get() + " ");
            }

            System.out.println("len after: " + len);
            List<Byte> wins = new ArrayList<Byte>();
            List<Byte> loses = new ArrayList<Byte>();
            List<Byte> ties = new ArrayList<Byte>();
         
            byte tie = 3;
            byte win;
            byte lose;
            if (OTurn) {
         
                win = 1;
                lose = 2;
             
                for (int i = 0; i < len; i++) {
                    byte val = bites.get(i).get();
                    byte status = (byte)(val << 6);
                    status = (byte)(status >>> 6);
                    System.out.println("Status: " + status);
                    if (status == win) {
                        val = (byte)(val >>> 2);
                        wins.add(val);
                    } else if (status == tie) {
                        val = (byte)(val >>> 2);
                        ties.add(val);
                    } else if (status == lose) {
                        val = (byte)(val >>> 2);
                        loses.add(val);
                    } else {
                        throw new IOException();
                    }
                }
             
                if (wins.size() > 0) {
                    Collections.sort(wins);
                    byte bst = (byte)((byte)(wins.get(0) << 2) | win);
                    return bst;
                } else if (ties.size() > 0) {
                    Collections.sort(ties);
                    byte bst = (byte)((byte)(ties.get(ties.size() - 1) << 2) | tie);
                    return bst;
                } else {
                    Collections.sort(loses);
                    byte bst = (byte)((byte)(loses.get(loses.size() - 1) << 2) | lose);
                    return bst;
                }
             
            } else {
         
                win = 2;
                lose = 1;
         
                for (int i = 0; i < len; i++) {
                    byte val = bites.get(i).get();
                    byte status = (byte)(val << 6);
                    status = (byte)(status >>> 6);
                    if (status == win) {
                        wins.add(bites.get(i).get());
                    } else if (status == tie) {
                        ties.add(bites.get(i).get());
                    } else if (status == lose) {
                        loses.add(bites.get(i).get());
                    } else {
                        throw new IOException();
                    }
                }
         
                if (wins.size() > 0) {
                    Collections.sort(wins);
                    byte bst = (byte)((byte)(wins.get(0) << 2) | win);
                    return bst;
                } else if (ties.size() > 0) {
                    Collections.sort(ties);
                    byte bst = (byte)((byte)(ties.get(ties.size() - 1) << 2) | tie);
                    return bst;
                } else {
                    Collections.sort(loses);
                    byte bst = (byte)((byte)(loses.get(loses.size() - 1) << 2) | lose);
                    return bst;
                }
            }         
        }
    }
}
