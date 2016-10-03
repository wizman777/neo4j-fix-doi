package org.rdswitchboard.utils.neo4j.doi;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.rdswitchboard.utils.neo4j.sync.exceptions.Neo4jException;

public class App {
	
	private static final String PROPERTY_DOI = "doi";
	
    private static final String PART_DOI_PERFIX = "doi:";
    private static final String PART_DOI_URI = "dx.doi.org/";

	private static final String NEO4J_CONF = "/conf/neo4j.conf";
	private static final String NEO4J_DB = "/data/databases/graph.db";
	
	private static File GetDbPath(final String folder) throws Neo4jException, IOException
	{
		File db = new File(folder, NEO4J_DB);
		if (!db.exists())
			db.mkdirs();
				
		if (!db.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return db;
	}
	
	private static File GetConfPath(final String folder) throws Neo4jException
	{
		File conf = new File(folder, NEO4J_CONF);
		if (!conf.exists() || conf.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return conf;
	}	
	
	private static GraphDatabaseService getReadOnlyGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "true" )
				.newGraphDatabase();
			
			registerShutdownHook( graphDb );
			
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	private static GraphDatabaseService getGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.newGraphDatabase();
		
			registerShutdownHook( graphDb );
		
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
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
	    });
	}
	
	private static String extractDoi(String str) {
		if (StringUtils.isNotEmpty(str)) {
			int pos = str.indexOf(PART_DOI_PERFIX);
			if (pos >= 0) 
				str = str.substring(pos + PART_DOI_PERFIX.length());
			
			pos = str.indexOf(PART_DOI_URI);
			if (pos >= 0) 
				str = str.substring(pos + PART_DOI_URI.length());
			
    		Matcher matcher = patternDoi.matcher(str);
    		if (matcher.find()) 
    			return matcher.group();
    	}
    	
		return null;
	}
	
	public static void main(String[] args) {
		try {
			String neo4j = "neo4j";
			if (args.length > 0 && !StringUtils.isEmpty(args[0])) {
				neo4j = args[0];
			}
			GraphDatabaseService graphDb = getGraphDb(neo4j);
			
			try ( Transaction tx = graphDb.beginTx() ) {
				for (Node node : graphDb.getAllNodes()) {
					if (node.hasProperty(PROPERTY_DOI)) {
						Object doi = node.getProperty(PROPERTY_DOI);
						
						if (doi instanceof String) {
							String _doi = extractDoi((String) doi);
							if (null != _doi && !_doi.equals(doi)) {
								node.setProperty(PROPERTY_DOI, _doi);
							}
							
						} else if (doi instanceof String[]) {
							String[] _doi = Arrays.stream((String)[] doi)
									.map((doi) -> extractDoi(doi))
									.toArray(String[]::new);
							
							if (null != _doi && !_doi.equals(doi)) {
								node.setProperty(PROPERTY_DOI, _doi);
							}
						}
					}
				}
				
				tx.success();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}	


}
