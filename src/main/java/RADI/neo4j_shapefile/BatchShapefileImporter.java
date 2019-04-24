package RADI.neo4j_shapefile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.measure.quantity.VolumetricDensity;

import java.util.Date;

import org.geotools.data.PrjFileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.shp.JTSUtilities;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.geotools.data.shapefile.shp.ShapefileReader.Record;
import org.neo4j.cypher.internal.compiler.v2_3.docgen.logicalPlanDocGen;
import org.neo4j.gis.spatial.Constants;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.WKBGeometryEncoder;
import org.neo4j.gis.spatial.rtree.Listener;
import org.neo4j.gis.spatial.rtree.NullListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.api.StackingQueryRegistrationOperations;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.neo4j.gis.spatial.OrderedEditableLayer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;



public class BatchShapefileImporter implements Constants {
	
	private int commitInterval;
	private boolean maintainGeometryOrder = false;

	public BatchShapefileImporter(GraphDatabaseService database, Listener monitor, int commitInterval, boolean maintainGeometryOrder) {	
		this.maintainGeometryOrder = maintainGeometryOrder;
        if (commitInterval < 1) {
            throw new IllegalArgumentException("commitInterval must be > 0");
        }
        this.commitInterval = commitInterval;
		this.database = database;
		this.spatialDatabase = new SpatialDatabaseService(database);
		
		if (monitor == null) monitor = new NullListener();
		this.monitor = monitor;
	}
	
	public BatchShapefileImporter(GraphDatabaseService database, Listener monitor, int commitInterval) {
		this(database, monitor, commitInterval, false);
	}

	public BatchShapefileImporter(GraphDatabaseService database, Listener monitor) {
		this(database, monitor, 1000, false);
	}

	public BatchShapefileImporter(GraphDatabaseService database) {	
		this(database, null, 1000, false);
	}
	



//    public List<Node> importFile(String dataset, String layerName) throws IOException {
//        return importFile(dataset, layerName, Charset.defaultCharset());
//    }

//    public List<Node> importFile(String dataset, String layerName, Charset charset) throws IOException {
//        Class<? extends Layer> layerClass = maintainGeometryOrder ? OrderedEditableLayer.class : EditableLayerImpl.class;
//        EditableLayerImpl layer = (EditableLayerImpl) spatialDatabase.getOrCreateLayer(layerName, WKBGeometryEncoder.class, layerClass);
//        return importFile(dataset, layer, charset);
//    }

    public List<Node> importFile(String dataset, EditableLayerImpl layer, Charset charset) throws IOException {
        GeometryFactory geomFactory = layer.getGeometryFactory();
		ArrayList<Node> added = new ArrayList<>();
		
		long startTime = System.currentTimeMillis();
		
		ShpFiles shpFiles;
		try {
			shpFiles = new ShpFiles(new File(dataset));
		} catch (Exception e) {
			try {
				shpFiles = new ShpFiles(new File(dataset + ".shp"));
			} catch (Exception e2) {
				throw new IllegalArgumentException("Failed to access the shapefile at either '" + dataset + "' or '" + dataset + ".shp'", e);
			}
		}
		
		ShapefileReader shpReader = new ShapefileReader(shpFiles, false, true, geomFactory);
		try {
            Class geometryClass = JTSUtilities.findBestGeometryClass(shpReader.getHeader().getShapeType());
            int geometryType = SpatialDatabaseService.convertJtsClassToGeometryType(geometryClass);
			
			// TODO ask charset to user?
			DbaseFileReader dbfReader = new DbaseFileReader(shpFiles, true, charset);
			try {
				DbaseFileHeader dbaseFileHeader = dbfReader.getHeader();
	            
				String[] fieldsName = new String[dbaseFileHeader.getNumFields()+1];
				fieldsName[0] = "ID";
				for (int i = 1; i < fieldsName.length; i++) {
					fieldsName[i] = dbaseFileHeader.getFieldName(i-1);
				}
				
				Transaction tx = database.beginTx();
				try {
                    CoordinateReferenceSystem crs = readCRS(shpFiles, shpReader);
                    if (crs != null) {
						layer.setCoordinateReferenceSystem(crs);
					}

					layer.setGeometryType(geometryType);
					layer.setExtraPropertyNames(fieldsName);
					tx.success();
				} finally {
					tx.close();
				}
				
				monitor.begin(dbaseFileHeader.getNumRecords());
				try {
					Record record;
					Geometry geometry;
					Object[] values;
                    ArrayList<Object> fields = new ArrayList<>();
					int recordCounter = 0;
					int filterCounter = 0;
					while (shpReader.hasNext() && dbfReader.hasNext()) {
						tx = database.beginTx();
						try {
							int committedSinceLastNotification = 0;
							for (int i = 0; i < commitInterval; i++) {
								if (shpReader.hasNext() && dbfReader.hasNext()) {
									record = shpReader.nextRecord();
									recordCounter++;
									committedSinceLastNotification++;
									try {
                                        fields.clear();
										geometry = (Geometry) record.shape();
										if (filterEnvelope == null || filterEnvelope.intersects(geometry.getEnvelopeInternal())) {
											values = dbfReader.readEntry();
                                                                                        
                                                                                        //convert Date to String 
                                                                                        //necessary because Neo4j doesn't support Date properties on nodes
                                                                                        for (int k = 0; k < fieldsName.length - 1; k++){
                                                                                            if (values[k] instanceof Date){
                                                                                                Date aux = (Date) values[k];
                                                                                                values[k] = aux.toString();
                                                                                            }
                                                                                        }
                                                                                        
											fields.add(recordCounter);
											Collections.addAll(fields, values);
											if (geometry.isEmpty()) {
												log("warn | found empty geometry in record " + recordCounter);
											} else {
												// TODO check geometry.isValid()
												// ?
												SpatialDatabaseRecord spatial_record = layer.add(geometry, fieldsName, fields.toArray(values));
												//added.add(spatial_record.getGeomNode());
											}
										} else {
											filterCounter ++;
										}
									} catch (IllegalArgumentException e) {
										// org.geotools.data.shapefile.shp.ShapefileReader.Record.shape() can throw this exception
										log("warn | found invalid geometry: index=" + recordCounter, e);
									}
								}
							}
							monitor.worked(committedSinceLastNotification);
							tx.success();

							log("info | inserted geometries: " + (recordCounter-filterCounter));
							if (filterCounter > 0) {
								log("info | ignored " + filterCounter + "/" + recordCounter
										+ " geometries outside filter envelope: " + filterEnvelope);
							}

						} finally {
							tx.close();
						}
					}
				} finally {
					monitor.done();
				}
			} finally {
				dbfReader.close();
			}			
		} finally {
			shpReader.close();
		}

		long stopTime = System.currentTimeMillis();
		log("info | elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000));
		return added;
	}
	
	
	// Private methods
	
	private CoordinateReferenceSystem readCRS(ShpFiles shpFiles, ShapefileReader shpReader) {
		try {
            PrjFileReader prjReader = new PrjFileReader(shpFiles.getReadChannel(ShpFileType.PRJ, shpReader));
            try {
            	return prjReader.getCoordinateReferenceSystem();
			} finally {
				prjReader.close();
			}
		} catch (IOException | FactoryException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private void log(String message) {
		System.out.println(message);
	}

	private void log(String message, Exception e) {
		System.out.println(message);
		e.printStackTrace();
	}
	
	
	// Attributes
	
	private Listener monitor;
	private GraphDatabaseService database;
	private SpatialDatabaseService spatialDatabase;
	private Envelope filterEnvelope;

	public void setFilterEnvelope(Envelope filterEnvelope) {
		this.filterEnvelope = filterEnvelope;
	}
	
//	public void progressShow(int value,int count,String p) throws IOException
//	{
//
//		String progessBar="info | progress:[";
//		for(int i=0;i<100;i++)
//		{
//			if(i<(value/(count*1.0)*100))
//			{
//				progessBar+=p;
//			}
//			else{
//				progessBar+=" ";
//			}		
//		}
//		progessBar+="] "+(value/(count*1.0)*100)+"%["+value+"/"+count+"]\r";
//		System.out.print(progessBar);
//	}
	public void BatchLayerImpoter(ArrayList<String>layers) throws IOException{
		Class<? extends Layer> layerClass = maintainGeometryOrder ? OrderedEditableLayer.class : EditableLayerImpl.class;
		//for(int count=0;count<2000;count++)
		for(int count=0;count<layers.size();count++)
        {	
        	//String layerName=layers.get(count);
        	//String layerName="RainStateLayer";
			//String layerName="RainEventLayer";
			String layerName="SstLayer";
        	long t1=System.currentTimeMillis();
        	EditableLayerImpl layer = (EditableLayerImpl) spatialDatabase.getOrCreateLayer(layerName, WKBGeometryEncoder.class, layerClass);
        	long t2=System.currentTimeMillis();
        	log("info | create or get layer time in seconds: " + (1.0 * (t2 - t1) / 1000));
        	log("info | Process: "+(count+1)+"/"+layers.size());
        	log("");
        }
	}
	
	 public void importFile(String path,ArrayList<String>files,String Layer,Charset charset) throws IOException {
		 	Class<? extends Layer> layerClass = maintainGeometryOrder ? OrderedEditableLayer.class : EditableLayerImpl.class;
	        for(int count=0;count<files.size();count++)

	        {
	        	String dataset="/"+path+"/"+files.get(count);	   	
	        	String layerName=Layer;
	        	long t1=System.currentTimeMillis();
	        	EditableLayerImpl layer = (EditableLayerImpl) spatialDatabase.getOrCreateLayer(layerName, WKBGeometryEncoder.class, layerClass);
	        	long t2=System.currentTimeMillis();
		        GeometryFactory geomFactory = layer.getGeometryFactory();			
				long startTime = System.currentTimeMillis();		
				ShpFiles shpFiles;
				try {
					shpFiles = new ShpFiles(new File(dataset));
				} catch (Exception e) {
					try {
						shpFiles = new ShpFiles(new File(dataset + ".shp"));
					} catch (Exception e2) {
						throw new IllegalArgumentException("Failed to access the shapefile at either '" + dataset + "' or '" + dataset + ".shp'", e);
					}
				}
				
				ShapefileReader shpReader = new ShapefileReader(shpFiles, false, true, geomFactory);
				try {
		            Class geometryClass = JTSUtilities.findBestGeometryClass(shpReader.getHeader().getShapeType());
		            int geometryType = SpatialDatabaseService.convertJtsClassToGeometryType(geometryClass);
					
					// TODO ask charset to user?
					DbaseFileReader dbfReader = new DbaseFileReader(shpFiles, true, charset);
					try {
						DbaseFileHeader dbaseFileHeader = dbfReader.getHeader();
			            
						String[] fieldsName = new String[dbaseFileHeader.getNumFields()+1];
						fieldsName[0] = "ID";
						for (int i = 1; i < fieldsName.length; i++) {
							fieldsName[i] = dbaseFileHeader.getFieldName(i-1);
						}
						
						Transaction tx = database.beginTx();
						try {
		                    CoordinateReferenceSystem crs = readCRS(shpFiles, shpReader);
		                    if (crs != null) {
								layer.setCoordinateReferenceSystem(crs);
							}

							layer.setGeometryType(geometryType);
							layer.setExtraPropertyNames(fieldsName);
							tx.success();
						} finally {
							tx.close();
						}
						
						monitor.begin(dbaseFileHeader.getNumRecords());
						try {
							Record record;
							Geometry geometry;
							Object[] values;
		                    ArrayList<Object> fields = new ArrayList<>();
							int recordCounter = 0;
							int filterCounter = 0;
							while (shpReader.hasNext() && dbfReader.hasNext()) {
								tx = database.beginTx();
								try {
									int committedSinceLastNotification = 0;
									for (int i = 0; i < commitInterval; i++) {
										if (shpReader.hasNext() && dbfReader.hasNext()) {
											record = shpReader.nextRecord();
											recordCounter++;
											committedSinceLastNotification++;
											try {
		                                        fields.clear();
												geometry = (Geometry) record.shape();
												if (filterEnvelope == null || filterEnvelope.intersects(geometry.getEnvelopeInternal())) {
													values = dbfReader.readEntry();
		                                                                                        
		                                                                                        //convert Date to String 
		                                                                                        //necessary because Neo4j doesn't support Date properties on nodes
		                                                                                        for (int k = 0; k < fieldsName.length - 1; k++){
		                                                                                            if (values[k] instanceof Date){
		                                                                                                Date aux = (Date) values[k];
		                                                                                                values[k] = aux.toString();
		                                                                                            }
		                                                                                        }
		                                                                                        
													fields.add(recordCounter);
													Collections.addAll(fields, values);
													if (geometry.isEmpty()) {
														log("warn | found empty geometry in record " + recordCounter);
													} else {
														// TODO check geometry.isValid()
														// ?
														SpatialDatabaseRecord spatial_record = layer.add(geometry, fieldsName, fields.toArray(values));
														//added.add(spatial_record.getGeomNode());
													}
												} else {
													filterCounter ++;
												}
											} catch (IllegalArgumentException e) {
												// org.geotools.data.shapefile.shp.ShapefileReader.Record.shape() can throw this exception
												log("warn | found invalid geometry: index=" + recordCounter, e);
											}
										}
									}
									monitor.worked(committedSinceLastNotification);
									tx.success();

									log("info | inserted geometries: " + (recordCounter-filterCounter));
									if (filterCounter > 0) {
										log("info | ignored " + filterCounter + "/" + recordCounter
												+ " geometries outside filter envelope: " + filterEnvelope);
									}

								} finally {
									tx.close();
								}
							}
						} finally {
							monitor.done();
						}
					} finally {
						dbfReader.close();
					}			
				} finally {
					shpReader.close();
				}
				long stopTime = System.currentTimeMillis();
				log("info | get layer time in seconds: " + (1.0 * (t2 - t1) / 1000));
				log("info | elapsed time in seconds: " + (1.0 * (stopTime - startTime) / 1000));
				log("info | Process: "+(count+1)+"/"+files.size());
				log("");

	        }
		
		}
}
