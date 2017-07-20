package com.neo4j.threadpool;

import java.util.TimerTask;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import com.codahale.metrics.Timer;
import com.jio.bootstrapmanager.MetricsManager;
import com.jio.bootstrapmanager.Neo4jBootstrapManager;

import myNeo4j.GraphCreation;
import myNeo4j.MyLabels;

public class ReadClass extends TimerTask {

	@Override
	public void run() {
		//	System.out.println("Called isnide  thread");
		long index=0;
		Timer.Context timerContext = MetricsManager.getInstance().getCreateNode().time();
		try(Transaction tx=GraphCreation.getInstance().dbService.beginTx())
		{
			/*if(Neo4jBootstrapManager.indexCount!=20000000)

			{ */ 
			for(int i=0;i<20000;i++)
			{
				if(Neo4jBootstrapManager.iterCount!=20000000)

				{
					index = Neo4jBootstrapManager.iterCount++;
					// first instance
					Node cachenode=	Neo4jBootstrapManager.getInstance().cache.get("A"+index);
					//	Node n= GraphCreation.getInstance().dbService.findNode(MyLabels.IMSI,"IMSI" ,"A"+index);
					///	n.setProperty("MSISDN", "Z"+index);
					//	cachenode.setProperty("MSISDN", "Y"+index);
					timerContext.close();
				}
				else Neo4jBootstrapManager.iterCount=0;
			//	else break;

			}

			tx.success(); // commit

			//	}

		}	catch(Exception e)
		{
			e.printStackTrace();
		}

		/*	try(Transaction tx=GraphCreation.getInstance().dbService.beginTx())
		{

			for(int i=0;i<20000;i++)
			{

				//	index = Neo4jBootstrapManager.indexCount++;
				// first instance
				Node cachenode=	Neo4jBootstrapManager.getInstance().cache.get("A"+i);
					Node n= GraphCreation.getInstance().dbService.findNode(MyLabels.IMSI,"IMSI" ,"A"+i);
				n.setProperty("MSISDN", "Z"+i);
				cachenode.setProperty("MSISDN", "B"+i);
				timerContext.close();


			}

			tx.success(); // commit



		}	catch(Exception e)
		{
			e.printStackTrace();
		}*/

	}
}

