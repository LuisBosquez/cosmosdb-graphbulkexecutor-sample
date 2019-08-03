package com.microsoft.cosmosdb.graphbulkexecutor.sample;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinEdge;
import com.microsoft.azure.documentdb.bulkexecutor.GraphBulkExecutor;
import java.util.List;

public class Main {

    private static final String HOST = "https://<endpoint>.documents.azure.com:443/";
    private static final String KEY  = "<key>";
    private static final String DATABASE_ID = "<DB_ID>";
    private static final String COLLECTION_ID = "<COLLECTION_ID>";

    public static void main( String[] args ) throws DocumentClientException {
        new Main().run();
    }

    private void run() throws DocumentClientException {
        System.out.println("Testing full insert using bulk executor .... ");
        testBulkExecutor();
    }

    private void testBulkExecutor() throws DocumentClientException {
        ConnectionPolicy directModePolicy = new ConnectionPolicy();
        directModePolicy.setConnectionMode(ConnectionMode.DirectHttps);
        DocumentClient client = new DocumentClient(HOST, KEY, directModePolicy, ConsistencyLevel.Eventual);
        DocumentCollection collection = client.readCollection(String.format("/dbs/%s/colls/%s", DATABASE_ID, COLLECTION_ID), null).getResource();
        GraphBulkExecutor.Builder graphBulkExecutorBuilder = GraphBulkExecutor.builder()
                .from(client, DATABASE_ID, COLLECTION_ID, collection.getPartitionKey(), Utils.getOfferThroughput(client, collection));

        try (GraphBulkExecutor executor = graphBulkExecutorBuilder.build()) {

            BulkDeleteResponse response = executor.deleteAll();
            System.out.println(String.format("Deleted all with total of %s object, total RUS: %s and latency: %s, errors: %s",
                    response.getNumberOfDocumentsDeleted(),
                    response.getTotalRequestUnitsConsumed(),
                    response.getTotalTimeTaken().toMillis(),
                    response.getErrors()));

            BulkImportResponse vResponse = executor.importAll(Utils.generateVertices(), true,true, 20);

            System.out.println(String.format("Finished vertices with RUSs: %s, latency: %s, total vertices: %s, errors: %s",
                    vResponse.getTotalRequestUnitsConsumed(),
                    vResponse.getTotalTimeTaken().toMillis(),
                    vResponse.getNumberOfDocumentsImported(),
                    vResponse.getErrors()));

            List<GremlinEdge> edges = Utils.generateEdges();
            BulkImportResponse eResponse = executor.importAll(edges, true, true, 20);

            System.out.println(String.format("Finished ingestion with RUSs: %s, latency: %s, total vertex %s, total edges %s, errors: %s",
                    eResponse.getTotalRequestUnitsConsumed(),
                    eResponse.getTotalTimeTaken().toMillis(),
                    eResponse.getNumberOfDocumentsImported(),
                    eResponse.getNumberOfDocumentsImported(),
                    eResponse.getErrors()));
        } catch (Exception e) {
            e.printStackTrace();
            // HANDLE EXCEPTION
        }

        client.close();
    }


}
