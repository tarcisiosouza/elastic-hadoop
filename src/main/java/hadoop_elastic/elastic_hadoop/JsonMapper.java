package hadoop_elastic.elastic_hadoop;

/**
 * Hello world!
 *
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.log4j.Logger;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.elasticsearch.hadoop.mr.EsOutputFormat;

//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;

public class JsonMapper {
	private static CdxRecord record;
	private static String Domain;
	private static HashMap<String,String> domainsCategories = new HashMap<String, String>();
public static class SampleMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable > { 
//	private final NullWritable outKey = NullWritable.get();
	private int test_number;
	
	 protected void setup(Context context) throws IOException, InterruptedException {
	        Path location2 = new Path("/user/souza/popdomains/popdomains.txt");
		    FileSystem fileSystem2 = location2.getFileSystem(context.getConfiguration());
		    
			RemoteIterator<LocatedFileStatus> fileStatusListIterator2 = fileSystem2.listFiles(
		            new Path("/user/souza/popdomains/popdomains.txt"), true);
	        
			while(fileStatusListIterator2.hasNext())
			{
		    	
				String line2;
		        LocatedFileStatus fileStatus = fileStatusListIterator2.next();
		        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem2.open(fileStatus.getPath())));		    
		        String final_token;
		        int valor = 0;
		        while ((line2 = br.readLine()) != null) 
	            {
		        	StringTokenizer matcher = new StringTokenizer(line2);
		        	String Key = matcher.nextToken();
		        	String Value = matcher.nextToken();
		        	domainsCategories.put(Key,Value);
	            }
			}

	/*    
	        Path location = new Path("/user/souza/stop/stopwords.txt");
		    FileSystem fileSystem = location.getFileSystem(context.getConfiguration());
		    
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
		            new Path("/user/souza/stop/stopwords.txt"), true);
	        
			while(fileStatusListIterator.hasNext())
			{
		    	
				String line1;
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));		    
	                
		        while ((line1 = br.readLine()) != null) 
	            {
	                	StringTokenizer token = new StringTokenizer (line1);
	                	String key = token.nextToken();
	                	
	                	stopwords.put(key, "1");
	            }
			}
	        vowels.put("a", "1");
	        vowels.put("e", "1");
	        vowels.put("i", "1");
	        vowels.put("o", "1");
	        vowels.put("u", "1");
		*/	
	    }

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

            MapWritable doc = new MapWritable();

//		  	GsonBuilder builder = new GsonBuilder();
//        	Gson capture = builder.create();
            if (value.toString().contains("filedesc:/DE"))
            	return;
         	record = new CdxRecord ();
         	try {
				record = parseLine (value.toString());
			} catch (Exception e) {
				return;
			}
        
         	if (record == null)
         		return;
         	try {
         		test_number=Integer.parseInt(record.getTs().substring(0, 4));
         	} catch (Exception e) {
         		return;
         	}
         	
         try {	
    		URL myUrl = new URL(record.getOrig());
    		//url = myUrl.getPath();
    		Domain = myUrl.getHost();
    		if (Domain.contains("www"))
     	   	{
     		   int index = Domain.indexOf(".");
         	   Domain = Domain.substring(index+1,Domain.length());
     	   	}
         } catch (Exception e)
         {
        	 return;
         }
    		if (!domainsCategories.containsKey(Domain))
    			return;
    	
//         	Text jsonDoc = new Text ();
//         	jsonDoc.set(record.toString());
//         	context.write(outKey, jsonDoc);
         	
         	doc.put(new Text("ts"), new Text(record.getTs()));
/*         	try {
				doc.put(new Text("domain"), new Text(record.getDomainName(record.getOrig())));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				doc.put(new Text("domain"), new Text(""));
			}
  */
         	doc.put(new Text("id"), new Text(record.getTs()+record.getOrig()));
//         	doc.put(new Text("mime"), new Text(""));
//         	doc.put(new Text("rescode"), new Text(""));
         	doc.put(new Text("orig"), new Text(record.getOrig()));
         	doc.put(new Text("redirectUrl"), new Text(record.getRedirectUrl()));
         	doc.put(new Text("compressedsize"), new Text(record.getCompressedsize()));
         	doc.put(new Text("offset"), new Text(record.getOffset()));
         	doc.put(new Text("filename"), new Text(record.getFilename()));
         	context.write(NullWritable.get(), doc);   
		}
	    
	
	
}
private static class CdxRecord {
	
//	private String url;
	private String ts;
	private String orig;
//	private String mime;
//	private String rescode;
//	private String checksum;
	private String redirectUrl;
//	private String meta;
	private String compressedsize;
	private String offset;
	private String filename;
//	private String domain;
//	private String keywords;
//	private String date;
//	private boolean isPopDomain;

	public String getDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

//	public void setDomain(String dom) {
//		domain = dom;
//	}

	public  String getTs() {
		return ts;
	}
	public  void setTs(String timestamp) {
	/*	if (timestamp.length() >14)
		{
			ts = timestamp.substring(0, 15);
		}
		else*/
			ts = timestamp;
	}
	public String getOrig() {
		return orig;
	}
	public  void setOrig(String o) {
		orig = o;
	}
//	public String getMime() {
//		return mime;
//	}
//	public  void setMime(String mimetype) {
//		mime = mimetype;
//	}
	
//	public  void setRescode(String resc) {
//		rescode = resc;
//	}

	
	public String getRedirectUrl() {
		return redirectUrl;
	}
	public void setRedirectUrl(String redirect) {
		redirectUrl = redirect;
	}
	
	
	public String getCompressedsize() {
		return compressedsize;
	}
	public void setCompressedsize(String comp) {
		compressedsize = comp;
	}
	public String getOffset() {
		return offset;
	}
	public void setOffset(String offs) {
		offset = offs;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String file) {
		filename = file;
	}

}

private static CdxRecord parseLine(String line) throws URISyntaxException { 
	
	StringTokenizer matcher = new StringTokenizer(line); 
	
//	matcher.nextToken();
	//record.setUrl (matcher.nextToken());
//	System.out.println(line);
	//matcher.nextToken();
	
	if (!matcher.hasMoreTokens())
		return null;
	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setTs("noTs");
	else
	record.setTs(matcher.nextToken());
	
	if (!matcher.hasMoreTokens())
		record.setOrig("noOrig");
	else
	record.setOrig(matcher.nextToken());
	
	matcher.nextToken();
	matcher.nextToken();
	matcher.nextToken();
/*		
	if (!matcher.hasMoreTokens())
		record.setMime("noMime");
	else
	record.setMime (matcher.nextToken());*/
	//record.setDomain(record.getDomainName (record.getOrig()));
	
	//matcher.nextToken();
	//record.setMime(matcher.nextToken());

	//record.setRescode(matcher.nextToken());
	//matcher.nextToken();
	
	//record.setChecksum(matcher.nextToken());
//	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setRedirectUrl("noRedirect");
	else
	record.setRedirectUrl(matcher.nextToken());
	matcher.nextToken();
//	matcher.nextToken();
	//record.setMeta(matcher.nextToken());
	//matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setCompressedsize("nocompressed");
	else
	record.setCompressedsize(matcher.nextToken());
	//matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setOffset("nooffset");
	else
	record.setOffset(matcher.nextToken());
//	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setFilename("nofilename");
	else
	record.setFilename(matcher.nextToken());
	//matcher.nextToken();
	return record; 
}


public static void main(String[] args) throws IOException,
InterruptedException, ClassNotFoundException {


Path inputPath = new Path(args[0]);
//Path outputDir = new Path(args[1]);

// Create configuration
//Configuration conf = new Configuration(true);
Configuration conf = new Configuration();
conf.set("es.nodes", "hadoop.kbs.uni-hannover.de"); //host
//conf.set("es.nodes", "master02.ib"); //host
conf.setInt("yarn.nodemanager.resource.memory-mb", 58000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.scheduler.maximum-allocation-mb", 58000);
conf.setInt("mapreduce.map.memory.mb", 3000);
conf.setInt("mapreduce.reduce.memory.mb", 6000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.app.mapreduce.am.resource.mb", 6000);
conf.set("mapreduce.map.java.opts", "-Xmx2400m");
conf.set("mapreduce.reduce.java.opts", "-Xmx4800m");
conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4800m");
//conf.setInt("mapreduce.task.io.sort.mb", 15);
conf.setInt("mapreduce.task.io.sort.mb", 1000);
conf.set("es.resource", "cdx-pop/capture");
conf.set("es.mapping.id", "id");
//conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//conf.set("es.input.json", "yes");
//conf.set(	"es.net.http.auth.user", "souza");
//conf.set("es.net.http.auth.pass", "pri2006");
conf.set("es.port", "9200"); // port//conf.setMapOutputValueClass(Text.class);
// Create jobr
//Job job = new Job(conf, "WordCount");

Job job = Job.getInstance(conf);
job.setJarByClass(JsonMapper.class);

// Setup MapReduce
job.setMapperClass(SampleMapper.class);
//job.setReducerClass(Reducer.class);
//job.setNumReduceTasks(0);

// Specify key / value
//job.setOutputKeyClass(NullWritable.class);

//job.setOutputValueClass(Text.class);

// Input
FileInputFormat.addInputPath(job, inputPath);
//job.setInputFormatClass(KeyValueTextInputFormat.class);
job.setInputFormatClass(TextInputFormat.class);

// Output
//FileOutputFormat.setOutputPath(job, outputDir);
//job.setOutputFormatClass(TextOutputFormat.class);
job.setOutputFormatClass(EsOutputFormat.class);
job.setSpeculativeExecution(false);
job.setOutputKeyClass(NullWritable.class);
job.setMapOutputValueClass(MapWritable.class);
//job.setOutputValueClass(Text.class);

// Delete output if exists
//FileSystem hdfs = FileSystem.get(conf);
//if (hdfs.exists(outputDir))
//hdfs.delete(outputDir, true);

//FileOutputFormat.setCompressOutput(job, true);
//FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
// Execute job
int code = job.waitForCompletion(true) ? 0 : 1;
System.exit(code);

}

}
