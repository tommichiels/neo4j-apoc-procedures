package apoc.algo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.codec.language.Metaphone;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import com.carrotsearch.hppc.LongHashSet;

import apoc.Description;
import apoc.result.CCResult;
import apoc.result.Empty;
import apoc.util.PerformanceLoggerSingleton;

public class EntityResolution {

	@Context
	public GraphDatabaseAPI db;

	@Context
	public Log log;

	private static final String query = "MATCH (n:Record)-->(l)<--(m:Record) where n <> m RETURN "
			+ "n.idKey as idKeya, n.name as namea, n.surname as surnamea,n.source as sourcea, "
			+ "m.idKey as idKeyb, m.name as nameb, m.surname as surnameb,m.source as sourceb";
	
	private static final String createWCC = "MERGE (n:cc {ccid:{ccid}}) SET n = {propertyMap} WITH n MATCH (m:record) where id(m)={id} WITH n,m MERGE (m)-[:partof]->(n) ";
	private static final String setSnapshotOnWCC ="MATCH (n:cc {ccid:{ccid}}) SET n = {propertyMap}";

	@Procedure("apoc.algo.ER")
	@Description("CALL apoc.algo.ER()")
	@PerformsWrites
	public Stream<Empty> ER() {

		Transaction tx=db.beginTx();
		try {
			log.info("starting ER");
			Metaphone meta = new Metaphone();
			PerformanceLoggerSingleton metrics = PerformanceLoggerSingleton.getInstance("/Users/tommichiels/Desktop/");
			Result result = db.execute(query);
			int counter = 0;
			while (result.hasNext()) {
				counter++;
				if (counter > 1000) {
					metrics.mark("TX");
					tx.success();
					tx.close();
					tx = db.beginTx();
					counter = 0;
				}
				metrics.mark("ER");
				Map<String, Object> row = result.next();
				String nameA = row.get("namea") != null ? row.get("namea").toString() : "";
				String nameB = row.get("nameb") != null ? row.get("nameb").toString() : "";
				String idKeyA = row.get("idKeya") != null ? row.get("idKeya").toString() : "";
				String surnameA = row.get("surnamea") != null ? row.get("surnamea").toString() : "";
				String surnameB = row.get("surnameb") != null ? row.get("surnameb").toString() : "";
				String idKeyB = row.get("idKeyb") != null ? row.get("idKeyb").toString() : "";
				if (compare(meta.encode(nameA), meta.encode(nameB))
						&& compare(meta.encode(surnameA), meta.encode(surnameB))) {
					log.info("MATCH (a:Record {idKey:'" + idKeyA + "'}) " + "MATCH (b:Record {idKey:'" + idKeyB
							+ "'}) " + "where NOT (a)-[:LINK]-(b) "
							+ "MERGE (a)-[:LINK {rulename:'NAME_SURNAME',strong:'TRUE'}]->(b);");
					db.execute("MATCH (a:Record {idKey:'" + idKeyA + "'}) " + "MATCH (b:Record {idKey:'" + idKeyB
							+ "'}) " + "where NOT (a)-[:LINK]-(b) "
							+ "MERGE (a)-[:LINK {rulename:'NAME_SURNAME',strong:'TRUE'}]->(b);");
			}
			}
			tx.success();
			log.info("ER done");
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
	
	
	@Procedure("apoc.algo.ERForID")
	@Description("CALL apoc.algo.ERForID()")
	@PerformsWrites
	public Stream<CCResult> ERForID(@Name("nodeId") Long nodeId) {
		try {
			List<List<Vertex>> results = new LinkedList<List<Vertex>>();
				List<Vertex> result = new LinkedList<Vertex>();
				List<Vertex> ccs = new LinkedList<Vertex>();
				long ccId=goLink(db.getNodeById(nodeId), Direction.BOTH);
				log.info("ERForID ccId: " + Long.toString(ccId));
				goCount(db.getNodeById(nodeId), Direction.BOTH, result,ccs);
				Map<String, Object> stats = result.stream().collect(Collectors.groupingBy(Vertex::getType)).entrySet().stream()
						.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().size()));
				
				// create or merge CC 
				 //TODO merge strategy 
				if(ccs.size()<=1){
					Map<String,Object> params = new HashMap<>();
					String type = "none";
					if (stats.containsKey("email") && stats.containsKey("address") && stats.containsKey("phone")){
						type="individual";
					}else if(stats.containsKey("email") || stats.containsKey("address") || stats.containsKey("phone")){
						type="contactable";
					}		
					stats.put("ccid", ccId);
					stats.put("type", type);
					params.put("ccid", ccId);
					params.put("id", nodeId);			
					params.put("propertyMap",stats);
					db.execute(createWCC,params);
					//db.execute(setSnapshotOnWCC,params);
				}else{
					 //TODO merge strategy 
				}
				
				
				results.add(result);
			log.info("calculation done");
			
			
			
			
			// it.close();
			// TODO fix later
			return results.stream()
					.map((x) -> new CCResult(ccId,
							x.stream().map((z) -> new Long(z.getId())).collect(Collectors.toList()),
							x.stream().collect(Collectors.groupingBy(Vertex::getType)).entrySet().stream()
									.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().size()))));
		} catch (Exception e) {
			String errMsg = "Error encountered while calculating weakly connected components";
			log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
		
		
		
		
		
		
		
	}
	
	
	private void goCount(Node node, Direction direction, List<Vertex> result, List<Vertex> ccs) {
		log.info("go Count");
		LongHashSet visitedIDs = new LongHashSet();
		List<Node> frontierList = new LinkedList<>();

		frontierList.add(node);
		visitedIDs.add(node.getId());
		storeNodeInMemory(node,result,ccs);
		
		while (!frontierList.isEmpty()) {
			node = frontierList.remove(0);
			Iterator<Node> it = getConnectedNodeIDs(node, direction).iterator();
			while (it.hasNext()) {
				Node child = it.next();
				if (visitedIDs.contains(child.getId())) {
					continue;
				}
				visitedIDs.add(child.getId());
				if (child.hasLabel(Label.label("record"))){
					frontierList.add(child);
				}
				storeNodeInMemory(child,result,ccs);
			}
		}
		return;
	}
	
	private void storeNodeInMemory(Node node,List<Vertex> result,List<Vertex> ccs){
		Iterator<Label> labelIt=node.getLabels().iterator();
		while(labelIt.hasNext()){
			String type=labelIt.next().name();
			if(type.equals("cc")){
				ccs.add(new Vertex(node.getId()+"",type));
			}else{
				result.add(new Vertex(node.getId()+"",type));
			}
		}
		
		Set<String> keySet = node.getAllProperties().keySet();
		if (keySet.size() > 1) {
			Iterator<String> attributeIt = keySet.iterator();
			while (attributeIt.hasNext()) {
				String type = attributeIt.next();
				result.add(new Vertex(node.getId() + "", type));
			}
		}
	}
	
	private long compareAndInsert(Node a,Node b)	{		
		log.info("starting ER for id");
		long ccId = (long) a.getProperty("ccid");
		
		Metaphone meta = new Metaphone();
		String lastnameA = a.hasProperty("lastname")  ? a.getProperty("lastname").toString() : "";
		String lastnameB = b.hasProperty("lastname") ? b.getProperty("lastname").toString() : "";
		String idKeyA = a.hasProperty("idKey")  ? a.getProperty("idKey").toString() : "";
		String firstnameA = a.hasProperty("firstname") ? a.getProperty("firstname").toString() : "";
		String firstnameB = b.hasProperty("firstname")  ? b.getProperty("firstname").toString() : "";
		String idKeyB = b.hasProperty("idKey")  ? b.getProperty("idKey").toString() : "";
		log.info(lastnameA + " "+lastnameB+ " "+idKeyA + " "+firstnameA+ " "+ firstnameB + " "+idKeyB) ;
		if (a.getId() != b.getId()
		   && compare(meta.encode(lastnameA), meta.encode(lastnameB))
		   && compare(meta.encode(firstnameA), meta.encode(firstnameB))) {
			long ccIdb = (long) b.getProperty("ccid");
			log.info("ccIdA: " + Long.toString(ccId));
			log.info("ccIdB: " + Long.toString(ccIdb));
			ccId=Math.min(ccId, ccIdb);
			log.info("min: " + Long.toString(ccId));
			log.info("MATCH (a:Record {idKey:'" + idKeyA + "'}) " + "MATCH (b:Record {idKey:'" + idKeyB
					+ "'}) " + "where NOT (a)-[:LINK]-(b) "
					+ "MERGE (a)-[:LINK {rulename:'NAME_SURNAME',strong:'TRUE'}]->(b);");
			db.execute("MATCH (a:record {idKey:'" + idKeyA + "'}) " + "MATCH (b:record {idKey:'" + idKeyB
					+ "'}) " + "where NOT (a)-[:LINK]-(b) "
					+ "MERGE (a)-[:LINK {rulename:'NAME_SURNAME',strong:'TRUE'}]->(b);");
	    }
		return ccId;
	}
	
	private long goLink(Node parent, Direction direction) {
		log.info("go Link");
		LongHashSet visitedIDs = new LongHashSet();
		List<Node> frontierList = new LinkedList<>();
		long ccId = (long) parent.getProperty("ccid");
		frontierList.add(parent);
		visitedIDs.add(parent.getId());

		while (!frontierList.isEmpty()) {
			Node node = frontierList.remove(0);
			Iterator<Node> it = getConnectedNodeIDs(node, direction).iterator();
			while (it.hasNext()) {
				Node child = it.next();
				if(parent.hasLabel(Label.label("record")) && child.hasLabel(Label.label("record"))){
				   ccId = Math.min(ccId, compareAndInsert(parent, child));
				   log.info("go link ccId: " + Long.toString(ccId));
				}
				if (visitedIDs.contains(child.getId())) {
					continue;
				}
				visitedIDs.add(child.getId());
				frontierList.add(child);
			}
		}
		return ccId;
	}
	
    
	private List<Node> getConnectedNodeIDs(Node node, Direction dir) {
		List<Node> it = new LinkedList<Node>();
		RelationshipType types=RelationshipType.withName("LINK");
		Iterator<Relationship> itR = node.getRelationships(types,dir).iterator();
		while (itR.hasNext()) {
			it.add(itR.next().getOtherNode(node));
		}
		RelationshipType typesLives=RelationshipType.withName("lives");
		Iterator<Relationship> itLives = node.getRelationships(typesLives,dir).iterator();
		while (itLives.hasNext()) {
			it.add(itLives.next().getOtherNode(node));
		}
		RelationshipType typesUses=RelationshipType.withName("uses");
		Iterator<Relationship> itUses = node.getRelationships(typesUses,dir).iterator();
		while (itUses.hasNext()) {
			it.add(itUses.next().getOtherNode(node));
		}
		return it;
	}

	private boolean compare(String recField1, String recField2) {
		if (!recField1.isEmpty() && !recField2.isEmpty()) {
			return recField1.equals(recField2);
		} else {
			return false;
		}
	}

}
