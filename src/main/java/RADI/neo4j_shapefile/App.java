package RADI.neo4j_shapefile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.gis.spatial.rtree.NullListener;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    
    	String strDBPath;
    	String strShpPath;
    	String strLayer;
    	String Error = "";
    	if(args.length!=3) {
    		return;
    	}
    	else
    	{
        	strDBPath=args[0];
        	strShpPath=args[1];
        	strLayer=args[2];
    	}

    	
    	File storeDir=new File(strDBPath);	
    	File shpPath=new File(strShpPath);
    	File []shpFiles=shpPath.listFiles(); 
    	ArrayList<String> shpArray=new ArrayList<String>();
    	for(int i=0;i<shpFiles.length;i++)
    	{
    		if(shpFiles[i].getName().endsWith("shp"))
    		{
    			shpArray.add(shpFiles[i].getName());
    		}
    	}  	
    	GraphDatabaseService databaseService=new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);
    	double time=0;
    	try {		   		
    		BatchShapefileImporter importer=new BatchShapefileImporter(databaseService,new NullListener(),1000);
    		long sT=System.currentTimeMillis();

    		long t1=System.currentTimeMillis();
    		String Path=shpPath.toString();
    		importer.importFile(Path,shpArray,strLayer,Charset.defaultCharset());
    		long t2=System.currentTimeMillis();
    		String MsgTime=""+((t2-t1)/1000.0/60);
    		MsgTime=MsgTime.substring(0,MsgTime.lastIndexOf('.')+2);
    		String Msg="Ellapsed Time: "+MsgTime+" min";
    		System.out.println(Msg);     		
    		System.exit(0);
    	}
    	catch(FileNotFoundException ex){
    		Error+=ex.getMessage()+"\r\n";
    	}
    	catch(Exception ex) {
       		Error+=ex.getMessage()+"\r\n";
    	}
    	finally {
    		databaseService.shutdown();
    	} 
    }	
}
