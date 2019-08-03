package com.microsoft.cosmosdb.graphbulkexecutor.sample;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinEdge;
import com.microsoft.azure.documentdb.bulkexecutor.graph.Element.GremlinVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Utils {
    private static final String EDGE_LABEL = "EDGE";
    private static final String VERTEX_LABEL = "VERTEX";
    private static final int NUMBER_OF_VERTICES = 20;
    private static final String PARTITION_KEY_PROPERTY_NAME = "vertexPartionKey";

    public static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        List<Offer> offerAsList = offers.getQueryIterable().toList();
        if (offerAsList.isEmpty()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerAsList.get(0);
        return offer.getContent().getInt("offerThroughput");
    }

    public static List<GremlinVertex> generateVertices()
    {
        List<GremlinVertex> vertices = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            String id = Integer.toString(i);
            GremlinVertex v = GremlinVertex.builder()
                    .setId(id)
                    .setLabel(VERTEX_LABEL)
                    .addProperty(PARTITION_KEY_PROPERTY_NAME, id)
                    .addProperty("vproperty", UUID.randomUUID().toString())
                    .build();

            vertices.add(v);
        }

        return vertices;
    }

    public static List<GremlinEdge> generateEdges() {
        List<GremlinEdge> batchEdges = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            for (int j = 0;j < NUMBER_OF_VERTICES; j++) {
                String from = Integer.toString(i);
                String to = Integer.toString(j);
                GremlinEdge.Builder builder = GremlinEdge.builder()
                        .setEdgeId(from + "-" + to)
                        .setEdgeLabel(EDGE_LABEL)
                        .setOutVertexId(from)
                        .setOutVertexPartitionKey(from)
                        .setOutVertexLabel(VERTEX_LABEL)
                        .setInVertexLabel(VERTEX_LABEL)
                        .setInVertexId(to)
                        .setInVertexPartitionKey(to);

                builder.addProperty("ep1", UUID.randomUUID().toString());
                batchEdges.add(builder.build());
            }
        }

        return batchEdges;
    }
}
