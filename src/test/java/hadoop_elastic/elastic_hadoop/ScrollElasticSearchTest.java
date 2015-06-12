package hadoop_elastic.elastic_hadoop;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class ScrollElasticSearchTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ScrollElasticSearchTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested	
     */
    public static Test suite()
    {
        return new TestSuite( ScrollElasticSearchTest.class );
    }

    public void testApp()
    {
    	/*
    	DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    	DateTime from = formatter.parseDateTime("2006-01-01T02:25:44.370Z");
    	DateTime from = new DateTime ("2006-01-01T02:25:44.370Z");
    	DateTime to = formatter.parseDateTime("2006-12-31T02:28:14.508Z");
    		
    	ScrollElasticSearch scrollTest = new ScrollElasticSearch ("master.hadoop", 9300, "hadoop",  "cdx-pop", "capture");
    	scrollTest.matchQueryTimestampFilter("orig", "\"fussball wm\"", from, to);
    	*/
    }
}
