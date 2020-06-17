package inter2ohdm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import util.DB;
import util.OHDM_DB;
import util.Parameter;
import util.SQLStatementQueue;
import util.Util;

/**
 *
 * @author thsc
 */
public class DebugInter2OHDM extends OHDMImporter {

    // TODO TODO... set updateStream!!
    public DebugInter2OHDM(IntermediateDB intermediateDB, Connection sourceConnection,
                           Connection targetConnection, String sourceSchema, String targetSchema) {

        super(intermediateDB, "2017-01-01", sourceConnection, targetConnection, sourceSchema, targetSchema, null);
    }
    
    public static void main(String args[]) throws IOException {
        SQLStatementQueue sourceQueue = null;
        
//        System.out.println(Util.getIntWithDots(12));
//        System.exit(0);
        
        try {
            /*
            String sourceParameterFileName = "db_inter_f4_test.txt";
            String targetParameterFileName = "db_ohdm_local.txt";
            */
            String sourceParameterFileName = "db_inter.txt";
            String targetParameterFileName = "db_ohdm.txt";
            
            if(args.length > 0) {
                sourceParameterFileName = args[0];
            }
            
            if(args.length > 1) {
                targetParameterFileName = args[1];
            }
            
//            Connection sourceConnection = Importer.createLocalTestSourceConnection();
//            Connection targetConnection = Importer.createLocalTestTargetConnection();
            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            Parameter targetParameter = new Parameter(targetParameterFileName);
            
            Connection sourceConnection = DB.createConnection(sourceParameter);
            Connection targetConnection = DB.createConnection(targetParameter);
            
            IntermediateDB iDB = new IntermediateDB(sourceConnection, sourceParameter.getSchema());
            
            String sourceSchema = sourceParameter.getSchema();
            String targetSchema = targetParameter.getSchema();
            
            SQLStatementQueue updateQueue = new SQLStatementQueue(sourceParameter);
            
            // TODO TODO remove that null
            OHDMImporter ohdmImporter = new OHDMImporter(iDB, targetParameter.getOsmfilecreationdate(),
                    sourceConnection,
                    targetConnection, sourceSchema, targetSchema, updateQueue);
            
            try {
//                ohdmImporter.forgetPreviousImport();
            }
            catch(Exception e) {
                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
            }
            
            try {
                OHDM_DB.dropOHDMTables(targetConnection, targetSchema);
            }
            catch(Exception e) {
                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
            }
            
            OHDM_DB.createOHDMTables(targetConnection, targetSchema);
            
            String stepLenString = sourceParameter.getReadStepLen();
            int stepLen = 10000;
            try {
                if(stepLenString != null) {
                    stepLen = Integer.parseInt(stepLenString);
                }
            }
            catch(NumberFormatException e) {
                    // ignore and work with default
            }

            OSMExtractor extractor = 
                    new OSMExtractor(sourceConnection, sourceSchema, ohdmImporter, stepLen);
            
            sourceQueue = DB.createSQLStatementQueue(sourceConnection, sourceParameter);
            
            ResultSet qResult = null; 
            // do some sql here..
//            sourceQueue.append("SELECT * FROM intermediate.nodes where osm_id = 6464945;");
            sourceQueue.append("SELECT * FROM germany.ways where osm_id = 236224563;");
//            sourceQueue.append("SELECT * FROM berlin.relations where osm_id = 173239;");
//            sourceQueue.append("SELECT * FROM intermediate.relations;");
            qResult = sourceQueue.executeWithResult();
            
            while(qResult.next()) {
                extractor.processElement(qResult, sourceQueue, OSMExtractor.WAY, true);
//                exporter.processElement(qResult, sourceQueue, OSMExtractor.RELATION, true);

                updateQueue.forceExecute();
                updateQueue.close();

            }
        } catch (Exception e) {
            Util.printExceptionMessage(e, sourceQueue, "main method in DebugInter2OHDM", false);
        }
    }
}
