package hadoop_elastic.elastic_hadoop;

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
	
	public void matchQueryTimestampFilter (String field, String terms, DateTime dateFrom, DateTime dateTo)
	{
		
			docs = 0;
			SearchResponse response = client.prepareSearch(index)
			        .setTypes(type)
			        .setSearchType(SearchType.SCAN)
			     //   .setSearchType(SearchType.QUERY_AND_FETCH)
			        .setScroll(new TimeValue(60000))
//			        .setQuery(QueryBuilders.filteredQuery(QueryBuilders.queryString(terms),
//			        FilterBuilders.rangeFilter("@timestamp").from("2006-01-01T02:25:44.370Z").to("2006-12-31T02:28:14.508Z")))
			        .setQuery(QueryBuilders.queryString(terms))             // Query
			        .setPostFilter(FilterBuilders.rangeFilter("ts").from("20060101022544").to("20061231022814"))   // Filter
//			        .setPostFilter(FilterBuilders.rangeFilter("ts").from(dateFrom.toString()).to(dateTo.toString()))   // Filter
			        .setSize(100).execute().actionGet();
			
			while (true) {
			    for (SearchHit hit : response.getHits().getHits()) {
			    	docs++;
			    	System.out.println (hit.getSourceAsString());
			    }
			    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
			    //Break condition: No hits are returned
			    if (response.getHits().getHits().length == 0) {
			    	System.out.println ("Docs:" + docs);
			        break;
			    }
			}
	}
}
