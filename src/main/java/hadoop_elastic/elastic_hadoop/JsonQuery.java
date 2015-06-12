package hadoop_elastic.elastic_hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class JsonQuery { 
	public static void main(String[] args) throws Exception 
	{ 
		//Path outputDir = new Path(args[0]);
		String jsonOutputPath = args[0]; //path where the output file will be stored
		String terms = args[1]; //query terms
		String dateFrom = args[2]; //initial timestamp 
		String dateTo = args[3]; //final timestamp
		Configuration conf = new Configuration(); 
		ScrollElasticSearch scroll = new ScrollElasticSearch ("master.hadoop", 9300, "hadoop",  "cdx-pop", "capture");
		scroll.matchQueryTimestampFilter(conf,jsonOutputPath,terms,dateFrom,dateTo);
	/*	
		conf.set("es.nodes", "hadoop.kbs.uni-hannover.de"); //host
		conf.set("es.port", "9200"); 
		conf.set("es.resource", "cdx-pop/capture");
		conf.set("es.query", jsonInputQueryPath.toString()); 
//		conf.set("es.query","{ \"query_string\" : { \"query\": \"Fussball 2006\"} }"); //orig: original url field 
		Job job = Job.getInstance(conf); 
		job.setJarByClass(JsonQuery.class); 
		job.setInputFormatClass(EsInputFormat.class); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(LinkedMapWritable.class);
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
		hdfs.delete(outputDir, true);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FileOutputFormat.setOutputPath(job, outputDir); 
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
 */
	} 
}
