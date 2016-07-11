package apoc.algo;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.codec.language.Metaphone;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.PerformsWrites;

import apoc.Description;
import apoc.result.Empty;
import apoc.util.PerformanceLoggerSingleton;

public class EntityResolutionCount {

	@Context
	public GraphDatabaseAPI db;

	@Context
	public Log log;

	private static final String query = "MATCH (n:Record)-->(l)<--(m:Record) where n <> m RETURN "
			+ "n.idKey as idKeya, n.name as namea, n.surname as surnamea,n.source as sourcea, "
			+ "m.idKey as idKeyb, m.name as nameb, m.surname as surnameb,m.source as sourceb";

	@Procedure("apoc.algo.ERCOUNT")
	@Description("CALL apoc.algo.ERCOUNT()")
	@PerformsWrites
	public Stream<Empty> ERCOUNT() {

		Transaction tx=db.beginTx();
		try {
			log.info("starting ER COUNT");
			PerformanceLoggerSingleton metrics = PerformanceLoggerSingleton.getInstance("/Users/tommichiels/Desktop/");
			Result result = db.execute(query);
			while (result.hasNext()) {
				result.next();
				metrics.mark("COUNT");
			}
			tx.success();
			log.info("ER COUNT done");
			// it.close();
			return Stream.empty();
		} catch (Exception e) {
			String errMsg = "Error encountered while doing ER job";
			tx.failure();
			log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		} finally {
			tx.close();
		}
	}

}
