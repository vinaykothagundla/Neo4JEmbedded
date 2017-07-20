package com.jio.bootstrapmanager;

import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.neo4j.logging.TestLogger;
import com.neo4j.pool.factory.PoolingManager;
import com.neo4j.threadpool.ClientThreadPoolExecutor;
import com.neo4j.threadpool.HelloWorldJob;
import com.neo4j.threadpool.QueueReaderThread;
import com.neo4j.threadpool.QueueWriterTimer;
import com.neo4j.threadpool.ReadClass;
import com.neo4j.threadpool.ReadNodeThread;
import com.neo4j.threadpool.SecondQueueReaderThread;

import myNeo4j.GraphCreation;
import myNeo4j.MyLabels;


/**
 * @author Ganmesh.Koli,Vinay.Kothagundla
 *
 */
public class Neo4jBootstrapManager {

	private static Neo4jBootstrapManager bootstrapManager = new Neo4jBootstrapManager();
	private static Logger logger = TestLogger.logger;
	public static long indexCount = 0L;
	public static long iterCount = 0L;
	private ClientThreadPoolExecutor threadPoolExecutor;
	public ExecutorService executorService;
	private PoolingManager poolingManager;
	private static PrintTimer printTimer;
	public static Timer timerPutDataInQueue = new Timer();
	static Timer timer = new Timer();
	private static int printTimerPeriodInSec = 1;
	public static long counter;
	public static long counterdGraphCreated;
	LinkedBlockingQueue<String[]> linkedBlockingQueue = new LinkedBlockingQueue();

	public static Timer timerGetDataFromQueue = new Timer();
	public static LoadingCache<String, Node> cache =  CacheBuilder.newBuilder()
			.maximumSize(20000000)
			.build(
					new CacheLoader<String, Node>() {

						@Override
						public Node load(String nodeId) throws Exception {
							//	System.out.println("nodeid>> " +nodeId );
							return getNameForNodeId(nodeId);
						}
					});
	private Neo4jBootstrapManager() {

	}

	public static Neo4jBootstrapManager getInstance() {
		if (bootstrapManager == null) {
			bootstrapManager = new Neo4jBootstrapManager();
		}
		return bootstrapManager;
	}

	public static void main(String[] args) {
		try {
			// TestLogger.initLogger();
			MetricsManager.getInstance();
			GraphCreation.getInstance();

			bootstrapManager.initialiseThreadPoolExecutor();
			bootstrapManager.poolingManager = new PoolingManager();

			//	createIndexes();
			//	createConstraints();

			//Thread.sleep(3000);

			/*System.out.println(System.currentTimeMillis());
			long startTime=System.currentTimeMillis();
			String a=warmUp();
			long endTime=System.currentTimeMillis();

			System.out.println(a);
			System.out.println((endTime-startTime));*/


			/*
			 * new Thread(new Runnable() {
			 * 
			 * @Override public void run() { try { bootstrapManager.readLoad();
			 * // get data from queue & // create graph } catch (Exception e) {
			 * e.printStackTrace(); } } }).start();
			 */

			/*	new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						QueueWriterTimer obj = new QueueWriterTimer();
						obj.readLoad(); //called only once
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
			 */
			/*	new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						bootstrapManager.startLoad(); // add data into queue
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();*/

			System.out.println("INvoking Thread");
			new Thread(new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					try
					{
						bootstrapManager.startReadLoad();
					}catch(Exception e)
					{
						e.printStackTrace();
					}

				}
			}).start();

			//Thread.sleep(1000);

			startPrintTimer();

			registerShutdownHook(GraphCreation.getInstance().dbService);
			//	System.out.println("size of link queue is " + Neo4jBootstrapManager.getInstance().getBlockingQueue().size());

			/*new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						bootstrapManager.readQuartzScheduler(); // add data into queue
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();*/

			//System.out.println("size of link queue is " + Neo4jBootstrapManager.getInstance().getBlockingQueue().size());


			System.out.println(" EXITTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT ");
			/*
			 * new Thread(new Runnable() {
			 * 
			 * @Override public void run() { try {
			 * bootstrapManager.readNodesFromDb(); // Readdata from // Graph DB
			 * } catch (Exception e) { e.printStackTrace(); } } }).start();
			 */


			// GraphCreation.getInstance().readNodes();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*	public static findAndUpdate()
	{




	}*/

	/**
	 * 
	 */
	private static void createConstraints() {

		ConstraintDefinition constraintDefinition;

		try ( Transaction tx = GraphCreation.getInstance().dbService.beginTx() )
		{

			Schema schema = GraphCreation.getInstance().dbService.schema();

			constraintDefinition = schema.constraintFor(MyLabels.IMSI)
					.assertPropertyIsUnique("IMSI")
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.GxIMSSessionID)
					.assertPropertyIsUnique( "GxIMSSessionID" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.GxInternetSessionID)
					.assertPropertyIsUnique( "GxInternetSessionID" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.IMSIP)
					.assertPropertyIsUnique( "IMSIP" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.InternetIP)
					.assertPropertyIsUnique( "InternetIP" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.SySessionID)
					.assertPropertyIsUnique( "SySessionID" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.RxVolte)
					.assertPropertyIsUnique( "RxVolte" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.RxVoip)
					.assertPropertyIsUnique( "RxVoip" )
					.create();

			constraintDefinition = schema.constraintFor(MyLabels.SdSessionId)
					.assertPropertyIsUnique( "SdSessionId" )
					.create();

			tx.success();
		}


	}

	private static Node getNameForNodeId(String nodeId) {
		final Node node =GraphCreation.getInstance().dbService.findNode(MyLabels.IMSI,"IMSI" ,nodeId);
		return (Node) node;
	}

	private void startReadLoad() throws Exception {

		timerPutDataInQueue.scheduleAtFixedRate(new ReadClass(), 1000L, 200L);

	}



	public static void createIndexes()
	{
		IndexDefinition indexDefinition;

		try ( Transaction tx = GraphCreation.getInstance().dbService.beginTx() )
		{

			Schema schema = GraphCreation.getInstance().dbService.schema();

			indexDefinition = schema.indexFor(MyLabels.IMSI)
					.on( "IMSI" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.GxIMSSessionID)
					.on( "GxIMSSessionID" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.GxInternetSessionID)
					.on( "GxInternetSessionID" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.IMSIP)
					.on( "IMSIP" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.InternetIP)
					.on( "InternetIP" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.SySessionID)
					.on( "SySessionID" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.RxVolte)
					.on( "RxVolte" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.RxVoip)
					.on( "RxVoip" )
					.create();

			indexDefinition = schema.indexFor(MyLabels.SdSessionId)
					.on( "SdSessionId" )
					.create();

			tx.success();
		}
		/*		
		try ( Transaction tx = GraphCreation.getInstance().dbService.beginTx() )
		{
		    Schema schema = GraphCreation.getInstance().dbService.schema();
		    schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
		}*/

		/*while(true)
		{

		try ( Transaction tx = GraphCreation.getInstance().dbService.beginTx() )
		{
		    Schema schema = GraphCreation.getInstance().dbService.schema();
		    System.out.println( String.format( "Percent complete: %1.0f%%",
		            schema.getIndexPopulationProgress( indexDefinition ).getCompletedPercentage() ) );
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		}*/


	}



	public static String warmUp() {

		try ( Transaction tx = 	GraphCreation.getInstance().dbService.beginTx()) {

			/*  for ( Node n : GraphCreation.getInstance().dbService.getAllNodes()) {

	            n.getPropertyKeys();

	            for ( Relationship relationship : n.getRelationships()) {

	                relationship.getStartNode();
	            }

	            //Relationship rs=n.getSingleRelationship(MyRelationshipTypes.IMS, Direction.OUTGOING);

	           // System.out.println(rs.getEndNode().getId());

	           // rs.getEndNode().setProperty("Name", "ANKIT");
	        }*/


		}
		return "Warmed up and ready to go!";
	}




	public static void readLoad() 
	{



		//System.out.println("Called");
		QueueReaderThread queueReaderThread = null;
		try {
			queueReaderThread = (QueueReaderThread) Neo4jBootstrapManager.getInstance().getPoolingManager()
					.getQueueReaderThreadPool().borrowObject();
			Neo4jBootstrapManager.getInstance().executorService.submit(queueReaderThread);			
			Neo4jBootstrapManager.getInstance().getPoolingManager().getQueueReaderThreadPool().returnObject(queueReaderThread);
		}

		catch (Exception e) {
			if (queueReaderThread != null) {
				try {
					Neo4jBootstrapManager.getInstance().getPoolingManager().getQueueReaderThreadPool()
					.returnObject(queueReaderThread);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		}
	}

	public void readQuartzScheduler()
	{

		try
		{
			JobDetail job = JobBuilder.newJob(HelloWorldJob.class)
					.withIdentity("dummyJobName", "group1").build();

			Trigger trigger = TriggerBuilder
					.newTrigger()
					.withIdentity("dummyTriggerName", "group1")
					.withSchedule(
							SimpleScheduleBuilder.simpleSchedule()
							.withIntervalInMilliseconds(100).repeatForever().withMisfireHandlingInstructionIgnoreMisfires())
					.build();

			Scheduler scheduler = new StdSchedulerFactory().getScheduler();
			scheduler.start();
			scheduler.scheduleJob(job, trigger);

		}catch(Exception e)
		{
			e.printStackTrace();
		}


	}


	public  void readLoadThread()
	{
		//SecondQueueReaderThread secondQueueReaderThread = null;

		try {


			timerGetDataFromQueue.scheduleAtFixedRate(new SecondQueueReaderThread(), 1000L, 250L);

			/*secondQueueReaderThread = (SecondQueueReaderThread) Neo4jBootstrapManager.getInstance().getPoolingManager()
					.getSecondQueueReaderThreadPool().borrowObject();

			Neo4jBootstrapManager.getInstance().getPoolingManager().getQueueReaderThreadPool().returnObject(secondQueueReaderThread);*/
		}

		catch (Exception e) {
			/*if (secondQueueReaderThread != null) {
				try {
					Neo4jBootstrapManager.getInstance().getPoolingManager().getSecondQueueReaderThreadPool()
					.returnObject(secondQueueReaderThread);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}*/
			e.printStackTrace();
		}
	}



	/*	public void readLoad() throws Exception {
		TimedSemaphore sem = new TimedSemaphore(1, TimeUnit.SECONDS, 128);
		while (true) {
			QueueReaderThread queueReaderThread = null;
			try {
				sem.acquire();
				queueReaderThread = (QueueReaderThread) this.getPoolingManager().getQueueReaderThreadPool()
						.borrowObject();
				this.executorService.submit(queueReaderThread);
			}

			catch (Exception e) {
				if (queueReaderThread != null) {
					this.getPoolingManager().getWriteThreadPool().returnObject(queueReaderThread);
				}
				e.printStackTrace();
			}
		}

	}*/

	public void initialiseThreadPoolExecutor() {

		try {
			int core = Runtime.getRuntime().availableProcessors() * 2;
			core=128;
			this.setThreadPoolExecutor(new ClientThreadPoolExecutor(core, core, 10000000, TimeUnit.HOURS,
					new LinkedBlockingQueue<Runnable>()));
			this.executorService = this.threadPoolExecutor;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void startPrintTimer() {
		printTimer = new PrintTimer();
		timer.schedule(printTimer, 1, printTimerPeriodInSec * 1000);
	}

	private void startLoad() throws Exception {
		// long startTime = System.currentTimeMillis()
		// WriteThread workerThread = null;

		// ExecutorService executorServiceForWrite =
		// Executors.newFixedThreadPool(64);

		timerPutDataInQueue.scheduleAtFixedRate(new QueueWriterTimer(), 1000L, 100L);

		/*
		 * 
		 * 
		 * for (int i = 0; i < 1; i++) { int startLimit = 50000000 * i; int
		 * endLimit = 50000000 * i + 50000000;
		 * TimedSemaphore sem = new TimedSemaphore(1, TimeUnit.SECONDS, 10);
		 * while(true){ endLimit = startLimit+3000; sem.acquire();
		 * QueueWriterThread queueWriterThread = (QueueWriterThread)
		 * this.getPoolingManager().getWriteThreadPool().borrowObject();
		 * queueWriterThread.setStartLimit(startLimit);
		 * queueWriterThread.setEndLimit(endLimit);
		 * //queueWriterThread.setSemaphore(sem);
		 * executorServiceForWrite.submit(queueWriterThread); startLimit =
		 * endLimit; 
		 * 
		 * 
		 */

		// executorServiceForWrite.shutdown();
		// executorServiceForWrite.awaitTermination(10, TimeUnit.HOURS);

		/*
		 * try {
		 * 
		 * for (int i = 0; i < 1000; i++) { workerThread = (WriteThread)
		 * this.getPoolingManager().getWriteThreadPool().borrowObject();
		 * this.executorService.submit(workerThread); counter++; }
		 * 
		 * // long endTime = System.currentTimeMillis() // System.out.println(
		 * "TOTAL TIME ****************** : " + // (endTime-startTime)) } catch
		 * (Exception e) { if (workerThread != null) {
		 * this.getPoolingManager().getWriteThreadPool().returnObject(
		 * workerThread); } e.printStackTrace(); }
		 */
		/*
		 * while (true) { new Thread(new Runnable() {
		 * 
		 * @Override public void run() { try { // Thread.sleep(2000);
		 * GraphCreation.getInstance().putDataInQueue("test"); } catch
		 * (Exception e) { e.printStackTrace(); } } }).start();}
		 */
	}
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
	}
	private void readNodesFromDb() throws Exception {
		// long startTime = System.currentTimeMillis()
		// WriteThread workerThread = null;

		ExecutorService executorServiceForRead = Executors.newFixedThreadPool(64);

		for (int i = 0; i < 64; i++) {
			int startLimit = 10000 * i;
			int endLimit = 10000 * i + 10000;

			executorServiceForRead.submit(new ReadNodeThread(startLimit, endLimit));
		}

		executorServiceForRead.shutdown();
		executorServiceForRead.awaitTermination(10, TimeUnit.HOURS);

	}

	public PoolingManager getPoolingManager() {
		return poolingManager;
	}

	public void setPoolingManager(PoolingManager poolingManager) {
		this.poolingManager = poolingManager;
	}

	public ClientThreadPoolExecutor getThreadPoolExecutor() {
		return threadPoolExecutor;
	}

	public void setThreadPoolExecutor(ClientThreadPoolExecutor threadPoolExecutor) {
		this.threadPoolExecutor = threadPoolExecutor;
	}

	public static long getCounterdGraphCreated() {
		return counterdGraphCreated;
	}

	public LinkedBlockingQueue<String[]> getBlockingQueue() {
		return linkedBlockingQueue;
	}

	public void setBlockingQueue(LinkedBlockingQueue<String[]> blockingQueue) {
		this.linkedBlockingQueue = blockingQueue;
	}

	public Timer getTimerPutDataInQueue() {
		return timerPutDataInQueue;
	}

	public void setTimerPutDataInQueue(Timer timerPutDataInQueue) {
		this.timerPutDataInQueue = timerPutDataInQueue;
	}

}
