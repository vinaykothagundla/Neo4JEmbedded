package myNeo4j;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import com.codahale.metrics.Timer;
import com.jio.bootstrapmanager.MetricsManager;
import com.jio.bootstrapmanager.Neo4jBootstrapManager;

public class GraphCreation {

	private static GraphCreation graphCreation;
	private static GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();;
	//	public static GraphDatabaseService dbService = dbFactory.newEmbeddedDatabase(new File("/rusers/ganmesh/graph.db/"));
	public static GraphDatabaseService dbService ;

	public int counter;

	private GraphCreation() {

		Map map = new HashMap<>();
		map.put("neostore.nodestore.db.mapped_memory", "4096M");
		map.put("neostore.relationshipstore.db.mapped_memory", "8192M");
		map.put("neostore.propertystore.db.mapped_memory", "8192M");
		map.put("neostore.propertystore.db.strings.mapped_memory", "2048M");
		map.put("neostore.propertystore.db.arrays.mapped_memory", "2048M");
		map.put("dbms.checkpoint.iops.limit", "-1");
		map.put("dbms.checkpoint.interval.tx","5000000");
		map.put("dbms.index_sampling.backgorund_enabled","false");
		map.put("dbms.logs.debug.level", "ERROR");
		map.put("dbms.logs.query.parameter_logging_enabled", "false");
		map.put("dbms.udc.enabled", "false");
		map.put("dbms.tx_log.rotation.size", "1g");
		map.put("dbms.checkpoint.interval.time", "3000s");
		map.put("dbms.tx_log.rotation.retention_policy", "false");
		map.put("dbms.logs.debug.rotation.delay", "60s");
		map.put("dbms.logs.debug.rotation.size", "5m");
		map.put("dbms.allow_format_migration","true");
		map.put("use_memory_mapped_buffers","true");
		map.put("cache_type","none");
		map.put("dbms.memory.pagecache.size", "20G");


		// 	dbService = dbFactory.newEmbeddedDatabaseBuilder(new File("/root/Vinay/neo4j-community-3.1.0/data/databases/graph.db/")).setConfig(map).newGraphDatabase();

		// 	dbService = dbFactory.newEmbeddedDatabaseBuilder(new File("/root/Vinay/neo4j-community-3.1.0/data/databases1/graph.db/")).setConfig(map).newGraphDatabase();

		dbService = dbFactory.newEmbeddedDatabaseBuilder(new File("/root/Vinay/neo4j-community-3.1.0/data/databases3/graph.db/")).setConfig(map).newGraphDatabase();

		//dbService = dbFactory.newEmbeddedDatabaseBuilder(new File("D:\\Neo4j1\\")).setConfig(map).newGraphDatabase();
	}

	public static GraphCreation getInstance() {
		if (graphCreation == null) {
			try {
				graphCreation = new GraphCreation();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return graphCreation;
	}

	/*public static void main(String[] args) {
		System.out.println("READING NODES");
		GraphCreation.getInstance().readNodes();
		// graphObj.traversalApi();

	}
	 */
	private void traversalApi() {
		try {

			Node nodeGanmesh = dbService.findNode(MyLabels.USERS, "Name", "Ganmesh");

			TraversalDescription traversalMoviesFriendsLike = dbService.traversalDescription()
					.relationships(MyRelationshipTypes.IS_FRIEND_OF)
					.relationships(MyRelationshipTypes.HAS_SEEN, Direction.OUTGOING).uniqueness(Uniqueness.NODE_GLOBAL)
					.evaluator(Evaluators.atDepth(2));
			Traverser traverser = traversalMoviesFriendsLike.traverse(nodeGanmesh); // specify
			// start
			// node
			Iterable<Node> moviesFriendsLike = traverser.nodes();
			for (Node movie : moviesFriendsLike) {
				System.out.println("Found movie: " + movie.getProperty("Name"));
			}

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	private void createGraph() {

		try (Transaction tx = dbService.beginTx()) {

			// creating nodes
			Node node1 = dbService.createNode(MyLabels.USERS);
			node1.setProperty("Name", "Ganmesh");
			node1.setProperty("year_of_birth", 1982); // int
			System.out.println("Node ID : " + node1);

			Node node2 = dbService.createNode(MyLabels.USERS);
			node2.setProperty("Name", "ARUN");
			node2.setProperty("locked", true); // boolean
			System.out.println("Node ID : " + node2);

			Node node3 = dbService.createNode(MyLabels.USERS);
			node3.setProperty("Name", "POOSHPENDU");
			node3.setProperty("cars_owned", new String[] { "BMW", "Audi" }); // array
			System.out.println("Node ID : " + node3);

			// creating relationships
			node1.createRelationshipTo(node2, MyRelationshipTypes.IS_FRIEND_OF);
			node1.createRelationshipTo(node3, MyRelationshipTypes.IS_FRIEND_OF);

			tx.success(); // commit

			Node movie1 = dbService.createNode();
			movie1.setProperty("Name", "FF1");
			System.out.println("Node ID movie1 : " + movie1);

			Node movie2 = dbService.createNode();
			movie2.setProperty("Name", "FF2");
			System.out.println("Node ID  movie2 : " + movie2);

			Node movie3 = dbService.createNode();
			movie3.setProperty("Name", "FF3");
			System.out.println("Node ID movie3 : " + movie3);

			Node movie4 = dbService.createNode();
			movie4.setProperty("Name", "FF4");
			System.out.println("Node ID movie3 : " + movie4);

			movie1.addLabel(MyLabels.MOVIE);
			movie2.addLabel(MyLabels.MOVIE);
			movie3.addLabel(MyLabels.MOVIE);

			Relationship rel1 = node1.createRelationshipTo(movie1, MyRelationshipTypes.HAS_SEEN);
			rel1.setProperty("Stars", 4);

			Relationship rel2 = node2.createRelationshipTo(movie1, MyRelationshipTypes.HAS_SEEN);
			rel2.setProperty("Stars", 3);

			Relationship rel3 = node2.createRelationshipTo(movie2, MyRelationshipTypes.HAS_SEEN);
			rel3.setProperty("Stars", 4);

			Relationship relx = node2.createRelationshipTo(movie4, MyRelationshipTypes.HAS_SEEN);
			relx.setProperty("Stars", 4);

			Relationship rel4 = node3.createRelationshipTo(movie3, MyRelationshipTypes.HAS_SEEN);
			rel4.setProperty("Stars", 5);

			if ("Ganmesh".equals(node1.getProperty("Name"))) {
				System.out.println(node1.getDegree());
			}

			// Find nodes with label movie
			ResourceIterator<Node> nodeWithLabel = dbService.findNodes(MyLabels.MOVIE);

			System.out.println("---------Searching Node with label-------------");

			while (nodeWithLabel.hasNext()) {
				System.out.println("Nodes with label : " + nodeWithLabel.next().getId());
			}

			System.out.println("---------Searching Node with label & prop-------------");

			ResourceIterator<Node> labelProp = dbService.findNodes(MyLabels.MOVIE, "name", "FF1");

			while (labelProp.hasNext()) {
				System.out.println("Nodes  with prop : " + labelProp.next().getId());
			}

			System.out.println("---------Searching Node with relationships------------");
			Set<Node> moviesForGanmesh = new HashSet<Node>();
			Node node = dbService.findNode(MyLabels.USERS, "Name", "Ganmesh");
			Iterable<Relationship> relationShip = node.getRelationships();
			for (Relationship reln : relationShip) {
				if (reln.getType().name().equalsIgnoreCase("HAS_SEEN")) {
					moviesForGanmesh.add(reln.getEndNode());
					System.out.println("Start node : " + reln.getStartNode());
					System.out.println("end node : " + reln.getEndNode().getProperties("name"));
				}
			}
			System.out.println("Movies seen :" + moviesForGanmesh.toString());

			System.out.println("---------Searching Node with relationships Directions------------");
			Set<Node> moviesForGanmeshDirn = new HashSet<Node>();
			Iterable<Relationship> relationShipDirn = node.getRelationships(Direction.OUTGOING,
					DynamicRelationshipType.withName("HAS_SEEN"));
			for (Relationship reln : relationShipDirn) {
				if (reln.getType().name().equalsIgnoreCase("HAS_SEEN")) {
					moviesForGanmeshDirn.add(reln.getEndNode());
					System.out.println("Start node : " + reln.getStartNode());
					System.out.println("end node : " + reln.getEndNode().getProperties("name"));
				}
			}
			System.out.println("Movies seen :" + moviesForGanmeshDirn.toString());
		}

	}

	public void createNodeTest() {

		try (Transaction tx = dbService.beginTx()) {

			// for (int i = 0; i < 100; i++) {
			Timer.Context timerContext = MetricsManager.getInstance().getCreateNode().time();

			Node node1 = dbService.createNode(MyLabels.USERS);
			node1.setProperty("Name", "IP");
			Node node2 = dbService.createNode(MyLabels.USERS);
			node2.setProperty("Name", "IMSI");
			Node node3 = dbService.createNode(MyLabels.USERS);
			node3.setProperty("Name", "MSISDN");
			Node node4 = dbService.createNode(MyLabels.USERS);
			node4.setProperty("Name", "SESSIONID");

			node1.createRelationshipTo(node2, MyRelationshipTypes.RELATE);
			node2.createRelationshipTo(node3, MyRelationshipTypes.RELATE);
			node3.createRelationshipTo(node4, MyRelationshipTypes.RELATE);
			node4.createRelationshipTo(node1, MyRelationshipTypes.RELATE);

			// Result resul = dbService.execute("create (IP:IP {name:'IP'}),
			// (IMSI:IMSI {name: 'IMSI'}), (MSISDN:MSISDN {name: 'MSISDN'}),
			// (SESSION_ID:SESSION_ID {name:
			// 'SESSION_ID'}),(IP)-[:RELATE]->(IMSI), (IP)-[:RELATE]->(MSISDN),
			// (IMSI)-[:RELATE]->(SESSION_ID),(MSISDN)-[:RELATE]->(SESSION_ID)");

			tx.success(); // commit

			// tx.close();

			timerContext.close();

			// Neo4jBootstrapManager.counterdGraphCreated++;

			// }

			/*
			 * new Thread(new Runnable() {
			 * 
			 * @Override public void run() {
			 * 
			 * tx.close();
			 * 
			 * } }).start();
			 */

		}
	}

	public void readNodes(int i) {

		try(Transaction tx = dbService.beginTx()) {
			Timer.Context timerContext = MetricsManager.getInstance().getCreateNode().time();
			ResourceIterator<Node> node = null;

			node = dbService.findNodes(MyLabels.USERS, "Name", "IMSI"+i);
			//long count = dbService.getAllNodes().stream().count();
			if(node!=null)
			{
				System.out.println("COUNT OF Node : "  + i +" = " + node.stream().count());	
				timerContext.close();
			}

			tx.success();


			/*try {
			System.out.println("######################CYPHER QUERY COUNT OF NODES###############");
			Result result = dbService.execute("match (n) return count(n) AS countNodes");
			while (result.hasNext()) {
				Map<String, Object> r = result.next();
				System.out.println("count " + r.get("countNodes"));
			}

		} catch (Exception e) {
			System.out.println(e);
		}*/

		}
	}

	public static void putDataInQueue(String[] data) throws InterruptedException {
		while (true) {
			long count = 0;
			long startTime = System.currentTimeMillis();
			while ((count < 10000)) {
				count++;
				Neo4jBootstrapManager.getInstance().getBlockingQueue().add(data);
				if ((startTime + 1000) < System.currentTimeMillis()) {
					Thread.sleep(200);
					break;
				}
			}

		}

	}

	public static void getDataFromQueue() throws Exception {

		while (true) {

			// for (int i = 0; i < 100; i++) {
			Timer.Context timerContext = MetricsManager.getInstance().getCreateNode().time();
			try (Transaction tx = dbService.beginTx()) {

				long time = System.currentTimeMillis();

				while (true) {
					String[] data = Neo4jBootstrapManager.getInstance().getBlockingQueue().poll();
					if (data != null) {
						Node node1 = dbService.createNode(MyLabels.USERS);
						node1.setProperty("Name", data[0]);
						Node node2 = dbService.createNode(MyLabels.USERS);
						node2.setProperty("Name", data[1]);
						Node node3 = dbService.createNode(MyLabels.USERS);
						node3.setProperty("Name", data[2]);
						Node node4 = dbService.createNode(MyLabels.USERS);
						node4.setProperty("Name", data[3]);
						node1.createRelationshipTo(node2, MyRelationshipTypes.RELATE);
						node2.createRelationshipTo(node3, MyRelationshipTypes.RELATE);
						node3.createRelationshipTo(node4, MyRelationshipTypes.RELATE);
						node4.createRelationshipTo(node1, MyRelationshipTypes.RELATE);
						// commit
						timerContext.close();
					}

					if ((System.currentTimeMillis() - time) >= 40) {
						tx.success();
						break;
					}
				}

				// }

			}

		}
	}

}