package com.microsoft.cosmosdb.graphbulkexecutor.sample;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkUpdateResponse;
import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinEdge;
import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinVertex;
import com.microsoft.azure.documentdb.bulkexecutor.graph.GraphBulkExecutor;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.microsoft.cosmosdb.graphbulkexecutor.sample.Constants.GREMLIN_QUERIES;
import static com.microsoft.cosmosdb.graphbulkexecutor.sample.Constants.PRECREATE_VERTEX_IDS;

public class Main {
    private static String HOST = "https://<endpoint>.documents.azure.com:443/";
    private static String KEY  = "<key>";
    private static String DATABASE_ID = "<DB_ID>";
    private static String COLLECTION_ID = "<COLLECTION_ID>";

    public static void main( String[] args ) throws ExecutionException, InterruptedException, DocumentClientException {
        new Main().run();
    }

    private void run() throws DocumentClientException, ExecutionException, InterruptedException {
        System.out.println("Testing using gremlin insert queries.... ");
        testGremlin();
        System.out.println();
        System.out.println();
        System.out.println("Testing full insert using bulk executor .... ");
        testBulkExecutor(false);
        System.out.println();
        System.out.println();
        System.out.println("Testing substitute edges drop and upsert using bulk executor.... ");
        testBulkExecutor(true);
        System.out.println();
        System.out.println();
        System.out.println("Testing update substitute edges using bulk executor.... ");
        testBulkUpdate();
        System.out.println();
        System.out.println();

    }

    private void testGremlin() throws ExecutionException, InterruptedException {
        /**
         * There typically needs to be only one Cluster instance in an application.
         */
        Cluster cluster;
        Client client;

        try {
            // Attempt to create the connection objects
            cluster = Cluster.build(new File("src/remote.yaml")).create();
            client = cluster.connect();
            runGremlinQuery(client, GREMLIN_QUERIES[0], "Running delete all: ");
            runGremlinQuery(client, GREMLIN_QUERIES[1], "Running ingestion for vertices:");
            runGremlinQuery(client, GREMLIN_QUERIES[2], "Running ingestion for vertices and edges:");

        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return;
        }

        cluster.close();
    }

    private void testBulkUpdate() throws DocumentClientException {
        ConnectionPolicy directModePolicy = new ConnectionPolicy();
        directModePolicy.setConnectionMode(ConnectionMode.DirectHttps);
        DocumentClient client = new DocumentClient(HOST, KEY, directModePolicy, ConsistencyLevel.Eventual);
        DocumentCollection collection = client.readCollection(String.format("/dbs/%s/colls/%s", DATABASE_ID, COLLECTION_ID), null).getResource();
        GraphBulkExecutor.Builder graphBulkExecutorBuilder = GraphBulkExecutor.builder()
                .from(client, DATABASE_ID, COLLECTION_ID, collection.getPartitionKey(), getOfferThroughput(client, collection));

        try (GraphBulkExecutor executor = graphBulkExecutorBuilder.build()) {
            BulkUpdateResponse updateResponse = executor.updateAll(readEdges("edges.graphson").stream().map((e) -> {
                e.addProperty("weight", 1.0);
                return e;
            }).collect(Collectors.toList()), 20);

            System.out.println(String.format("Finished update with RUSs: %s, latency: %s, total items updated: %s, errors: %s",
                    updateResponse.getTotalRequestUnitsConsumed(),
                    updateResponse.getTotalTimeTaken().toMillis(),
                    updateResponse.getNumberOfDocumentsUpdated(),
                    updateResponse.getErrors()));

        } catch (Exception e) {
            e.printStackTrace();
        }

        client.close();
    }

    private void testBulkExecutor(Boolean deleteEdgesOnly) throws DocumentClientException {
        ConnectionPolicy directModePolicy = new ConnectionPolicy();
        directModePolicy.setConnectionMode(ConnectionMode.DirectHttps);
        DocumentClient client = new DocumentClient(HOST, KEY, directModePolicy, ConsistencyLevel.Eventual);
        DocumentCollection collection = client.readCollection(String.format("/dbs/%s/colls/%s", DATABASE_ID, COLLECTION_ID), null).getResource();
        GraphBulkExecutor.Builder graphBulkExecutorBuilder = GraphBulkExecutor.builder()
                .from(client, DATABASE_ID, COLLECTION_ID, collection.getPartitionKey(), getOfferThroughput(client, collection));

        try (GraphBulkExecutor executor = graphBulkExecutorBuilder.build()) {
            if (deleteEdgesOnly) {
                final String sourceVertexId = "4GC5MFUS3IC6";
                BulkDeleteResponse response = executor.deleteEdgesByLabel(sourceVertexId, "substitute");

                System.out.println(String.format("Finished edges delete on vertex: %s, label: %s ; RUSs: %s, latency: %s, total items: %s, errors: %s",
                        sourceVertexId,
                        "substitute",
                        response.getTotalRequestUnitsConsumed(),
                        response.getTotalTimeTaken().toMillis(),
                        response.getNumberOfDocumentsDeleted(),
                        response.getErrors()));
            } else {
                BulkDeleteResponse response = executor.deleteAll();
                System.out.println(String.format("Deleted all with total of %s object, total RUS: %s and latency: %s, errors: %s",
                        response.getNumberOfDocumentsDeleted(),
                        response.getTotalRequestUnitsConsumed(),
                        response.getTotalTimeTaken().toMillis(),
                        response.getErrors()));

                // Pre-load
                BulkImportResponse preLoadVertices = executor.importAll(generateVertices(PRECREATE_VERTEX_IDS), true,true, 20);

                System.out.println(String.format("Finished data pre-load with RUSs: %s, latency: %s, total vertices: %s, errors: %s",
                        preLoadVertices.getTotalRequestUnitsConsumed(),
                        preLoadVertices.getTotalTimeTaken().toMillis(),
                        preLoadVertices.getNumberOfDocumentsImported(),
                        preLoadVertices.getErrors()));
            }

            List<GremlinVertex> vertices = readVertices("vertices.graphson");
            BulkImportResponse vResponse = executor.importAll(vertices, true, true, 20);

            List<GremlinEdge> edges = readEdges("edges.graphson");
            BulkImportResponse eResponse = executor.importAll(edges, true, true, 20);

            System.out.println(String.format("Finished ingestion with RUSs: %s, latency: %s, total vertex %s, total edges %s, errors: %s",
                    vResponse.getTotalRequestUnitsConsumed() + eResponse.getTotalRequestUnitsConsumed(),
                    vResponse.getTotalTimeTaken().toMillis() + eResponse.getTotalTimeTaken().toMillis(),
                    vResponse.getNumberOfDocumentsImported(),
                    eResponse.getNumberOfDocumentsImported(),
                    new ArrayList<>(vResponse.getErrors()).addAll(eResponse.getErrors())));
        } catch (Exception e) {
            e.printStackTrace();
        }

        client.close();
    }

    private static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        List<Offer> offerAsList = offers.getQueryIterable().toList();
        if (offerAsList.isEmpty()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerAsList.get(0);
        return offer.getContent().getInt("offerThroughput");
    }

    private List<GremlinVertex> generateVertices(String[] vertexIds)
    {
        List<GremlinVertex> vertices = new ArrayList<>();

        for (String vertexId: Arrays.asList(vertexIds))
        {
            GremlinVertex v = new GremlinVertex(vertexId, "product");
            v.addProperty("product_type", UUID.randomUUID().toString());
            v.addProperty("color", UUID.randomUUID().toString());
            v.addProperty("image_url", UUID.randomUUID().toString());
            v.addProperty("VertexKey", vertexId);
            v.addProperty("finish", UUID.randomUUID().toString());
            v.addProperty("name", UUID.randomUUID().toString());
            v.addProperty("facet_product_type", UUID.randomUUID().toString());
            v.addProperty("product_name", UUID.randomUUID().toString());

            vertices.add(v);
        }

        return vertices;
    }

    private List<GremlinVertex> readVertices(String fileName)
    {
        List<GremlinVertex> batchVertices = new ArrayList<>();

        try (FileInputStream stream = new FileInputStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

            GraphReader graphReader = GraphSONIo.build(GraphSONVersion.V1_0).graph(EmptyGraph.instance()).create().reader().create();

            String line = reader.readLine();
            while (line != null) {
                Vertex vertex = graphReader.readVertex(new ByteArrayInputStream(line.getBytes()), (a) -> a.get());

                GremlinVertex batchVertex = new GremlinVertex(vertex.id().toString(), vertex.label());

                vertex.properties().forEachRemaining((t) -> {
                    batchVertex.addProperty(t.label(), t.value());
                });

                batchVertices.add(batchVertex);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return batchVertices;
    }

    private List<GremlinEdge> readEdges(String fileName)
    {
        List<GremlinEdge> batchEdges = new ArrayList<>();

        try (FileInputStream stream = new FileInputStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

            GraphReader graphReader = GraphSONIo.build(GraphSONVersion.V1_0).graph(EmptyGraph.instance()).create().reader().create();

            String line = reader.readLine();
            while (line != null) {
                Edge edge = graphReader.readEdge(new ByteArrayInputStream(line.getBytes()), (a) -> a.get());

                GremlinEdge batchEdge = new GremlinEdge(
                        edge.id().toString(),
                        edge.label(),
                        edge.outVertex().id().toString(),
                        edge.inVertex().id().toString(),
                        edge.outVertex().label(),
                        edge.inVertex().label(),
                        edge.outVertex().id(),
                        edge.inVertex().id());

                edge.properties().forEachRemaining((t) -> {
                    batchEdge.addProperty(t.key(), t.value());
                });

                batchEdges.add(batchEdge);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return batchEdges;
    }

    private void runGremlinQuery(Client client, final String query, final String message) throws ExecutionException, InterruptedException {
        Stopwatch watch = Stopwatch.createStarted();
        ResultSet results = client.submit(query);

        CompletableFuture<List<Result>> completableFutureResults = results.all();
        List<Result> resultList = completableFutureResults.get();

        for (Result result : resultList) {
            System.out.println("\nQuery result:");
            System.out.println(result);
        }

        watch.stop();
        System.out.println(message);
        System.out.println("Total request charge = " + results.statusAttributes().get().get("x-ms-total-request-charge") + ", Latency = " + watch.elapsed().toMillis());
    }
}
