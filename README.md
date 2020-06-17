# OSMUpdateWizard with Postgres COPY












Edited: \n
  landusages.sql: \n
  -only convert polygons, not lines and points \n
    -only convert polygons with the correct subclassname as specified by OSM \n
    -only computing the ST_Area of polygons where ST_IsValid = '1' (true), preventing negative Areas so the conversion doesnt fail \n\n
    
  waterareas.sql: \n
    -only convert polygons with the correct classificationid \n
    -only computing the ST_Area of polygons where ST_IsValid = '1' (true), preventing negative Areas so the conversion doesnt fail \n
    
  OSMExtractor.java:
    -commented out: "System.err.println("could not find all members of relation (ok when not importing whole world): osm_id: " +   
                   relation.getOSMIDString());" 
                   
  AbstractElement.java: 
    -commented out: "System.err.println("null value for key (when deserializing attributes): " + key);"
    
  New Function:
    createSpatialIndex():
      -creates spataial indexes(GIST) for the tables: 
        MY_HOUSENUMBERS, MY_ADMIN_LABELS, MY_BOUNDARIES, MY_BUILDINGS, MY_AMENITIES, MY_LANDUSAGES, MY_PLACES, MY_ROADS, 
        MY_TRANSPORT_AREAS, MY_TRANSPORT_POINTS, MY_WATERAREA, MY_WATERWAYS for geometry column
  
  New Class: 
    IsRunningChecker.java:
      -checks every 5 minutes (sleep 300000) for the last state_change, PID and depending on postgreversion waiting or
       wait_event and wait_event_type for the specified connection parameters and StatementIdentifierString
       (StatementIdentifierString is used to identify the correct query if multiple queries are running on the server at the same time,
       if there are multiple queries containing the same IdentifierString the IsRunningChecker cant determine which is the right one you        you want information about)
      -operates in a own thread so isRunningChecker.interrupt() must be called after sql statement is finished 
      
      
      
    
  
  
  
  
    
        
