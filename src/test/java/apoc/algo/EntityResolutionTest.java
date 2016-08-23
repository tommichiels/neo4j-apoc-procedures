package apoc.algo;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EntityResolutionTest
{
    private static GraphDatabaseService db;
    //TODO refactor in multiple execute statements dirty workaround
    public static final String CC_GRAPH =
            "CREATE (:node:record { ccid: 1,  firstname : 'test', lastname : 'test' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:node:email { id: 1, email : 'test@gmail.com' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:node:record {ccid: 2, firstname : 'test', lastname : 'test' }) "+
            "WITH count(*) as dummy " +
            "CREATE (:node:email { id: 2 ,email : 'test2@gmail.com' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:node:record {ccid: 3, firstname : 'test', lastname : 'test' }) "+
            "WITH count(*) as dummy " +
            "MATCH (a:node),(b:node),(c:node),(d:node),(e:node)  "+
            "WHERE a.ccid = 1 AND b.id = 1 AND c.ccid = 2 AND d.id = 2 AND e.ccid = 3 "+
            "CREATE (a)-[:uses]->(b)<-[:uses]-(c)-[:uses]->(d)<-[:uses]-(e)";

    @BeforeClass
    public static void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, EntityResolution.class );
        db.execute( CC_GRAPH ).close();
    }

    @AfterClass
    public static void tearDown()
    {
        db.shutdown();
    }

    // ==========================================================================================

    @Test
    public void shouldReturnExpectedResultCountWhenUsingWeaklyConnected()
    {
        assertExpected( 5, "CALL apoc.algo.ERForID(0)" + "" );
    }
    
//    @Test
//    public void shouldReturnExpectedResultWhenUsingWeaklyConnected()
//    {
//        assertExpectedResultOfType( LabelCounter.class, "CALL apoc.algo.wcc()" + "" );
//    }
    
    private void assertExpected( int expectedResultCount, String query )
    {
        TestUtil.testCallCount( db, query, null,5 );
    }
    

    private void assertExpectedResultOfType( java.lang.Class<?> type , String query )
    {
        TestUtil.testResult( db, query, ( result ) -> {
            Object value = result.next().get( "value");   
        	assertThat( value, is( instanceOf( List.class ) ) );
        	assertThat( ((List)value).get(0), is( instanceOf( type ) ) );
        } );
    }

}

