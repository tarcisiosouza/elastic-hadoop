package hadoop_elastic.elastic_hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders.*;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.SearchHit;

public class ScrollElasticSearch {
	
	private TransportClient client;
	private Settings settings;
	private static String index;
	private static String type;
	private int docs;
	public ScrollElasticSearch(String hostname, int port, String cluster, String i, String t) 
	{
			index = i;
			type = t;
			settings = ImmutableSettings.settingsBuilder()
			    .put("cluster.name", cluster).build();
			client = new TransportClient(settings)
			    .addTransportAddress(new InetSocketTransportAddress(hostname, port));
	}
	
	public void matchQueryTimestampFilter (Configuration conf,String output, String terms, String dateFrom, String dateTo)
	{
		
			docs = 0;
			Path location = new Path(output);
		    FileSystem filesystem = null;
		    FSDataOutputStream out = null;
		    
		    try {
				filesystem = FileSystem.get(conf);
				out = filesystem.create(location);
			} catch (IOException e) {
				System.out.println ("IO exception");
			}
		    // 20060101022544 20061231022814
			SearchResponse response = client.prepareSearch(index)
			        .setTypes(type)
			        .setSearchType(SearchType.SCAN)
			     //   .setSearchType(SearchType.QUERY_AND_FETCH)
			        .setScroll(new TimeValue(60000))
//			        .setQuery(QueryBuilders.filteredQuery(QueryBuilders.queryString(terms),
//			        FilterBuilders.rangeFilter("@timestamp").from("2006-01-01T02:25:44.370Z").to("2006-12-31T02:28:14.508Z")))
			        .setQuery(QueryBuilders.queryString(terms))             // Query
			        .setPostFilter(FilterBuilders.rangeFilter("ts").from(dateFrom).to(dateTo))   // Filter
//			        .setPostFilter(FilterBuilders.rangeFilter("ts").from(dateFrom.toString()).to(dateTo.toString()))   // Filter
			        .setSize(100).execute().actionGet();
			
			while (true) {
			    for (SearchHit hit : response.getHits().getHits()) {
			    	
			    	try {
						out.writeBytes((hit.getSourceAsString()+"\n"));
						docs++;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
			    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
			    //Break condition: No hits are returned
			    if (response.getHits().getHits().length == 0) {
//			    	System.out.println ("Docs:" + docs);
			        break;
			    }
			}
	}
}
