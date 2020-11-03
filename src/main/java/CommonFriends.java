
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.util.*;
import java.io.IOException;

public class CommonFriends {
    static class SetFriendsAsKeysMapper extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(", ");
            String person = person_friends[0];
            String[] friends = person_friends[1].split(" ");
            //将friend写为key，以将指向每个friend的每个person作为value
            for(String friend : friends){
                context.write(new Text(friend), new Text(person));
            }
        }
    }

    static class SetFriendsAsKeysReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            StringBuilder persons = new StringBuilder();
            //以,分隔person，并以text方式输出
            for(Text friend : values){
                persons.append(friend.toString()).append(",");
            }
            persons.deleteCharAt(persons.length()-1);
            context.write(key, new Text(persons.toString()));
        }
    }

    static class SetPairsAsKeysMapper extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons);
            //嵌套遍历persons以得到输出的每个pair(作为key)
            for(int i=0;i<persons.length-1;i++){
                for(int j=i+1;j<persons.length;j++){
                    context.write(new Text("["+persons[i]+","+persons[j]+"]"), new Text(friend));
                }
            }
        }
    }

    static class SetPairsAsKeysReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            StringBuilder commmonFriends = new StringBuilder();
            //将每个pair对应的共同好友按照指定格式写入value
            for(Text friend:values){
                commmonFriends.append(friend.toString()+",");
            }
            commmonFriends.deleteCharAt(commmonFriends.length()-1);

            context.write(key, new Text("["+commmonFriends.toString()+"]"));
        }
    }
    //自定义输出格式
    static class DotOutputFormat extends TextOutputFormat<Text,Text>{
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            //将key和value的分隔符换为,
            String keyValueSeparator = ",";
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                codec =  ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }

            Path file = this.getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut = fs.create(file, false);
            return isCompressed ? new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator) : new LineRecordWriter(fileOut, keyValueSeparator);
        }
    }

    public static void main(String[] args)throws Exception {
        Configuration conf = new Configuration();
        String[] OtherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(OtherArgs.length!=2){
            System.err.println("type in <input> <output>\n");
            System.exit(2);
        }

        //job1,输出把每一个被指向的friend作为key,person作为value
        Job job1 = Job.getInstance(conf,"setFriendsAsKeys");
        job1.setJarByClass(CommonFriends.class);
        job1.setMapperClass(SetFriendsAsKeysMapper.class);
        job1.setReducerClass(SetFriendsAsKeysReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //中转文件夹
        Path midRes = new Path("midRes");
        FileInputFormat.setInputPaths(job1, new Path(OtherArgs[0]));
        FileOutputFormat.setOutputPath(job1, midRes);

        if(job1.waitForCompletion(true)){
            //job2，输出把每一个person对作为key，其共同好友作为value
            Job job2 = Job.getInstance(conf,"setPairsAsKeys");
            job2.setJarByClass(CommonFriends.class);
            job2.setMapperClass(SetPairsAsKeysMapper.class);
            job2.setReducerClass(SetPairsAsKeysReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            //使用自定义输出格式
            job2.setOutputFormatClass(DotOutputFormat.class);
            FileInputFormat.setInputPaths(job2, midRes);
            FileOutputFormat.setOutputPath(job2, new Path(OtherArgs[1]));
            if(job2.waitForCompletion(true)){
                //若两个job都成功，则退出时删除中转文件夹
                FileSystem.get(conf).deleteOnExit(midRes);
                System.exit(0);
            }
            else{
                System.out.println("job2 failed\n");
                System.exit(1);
            }
        }
        else{
            System.out.println("job1 failed\n");
            System.exit(1);
        }
    }
}