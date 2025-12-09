package com.couchbase.javaclient.doc;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonArray;
import com.github.javafaker.Faker;

/**
 * Hierarchical Vector document template for testing hierarchical vector search.
 *
 * Structure:
 * - Root level "embeddings": Array of arrays containing ALL 20 vectors (ground truth)
 * - Nested hierarchy: Same 20 vectors spread throughout company → departments → teams → members
 *
 * This allows:
 * - Ground truth access via: doc.embeddings[outer_idx][inner_idx]
 * - Search validation via nested paths: doc.company.departments[i].teams[j].embedding
 *
 * Vector layout:
 * - embeddings[0][0-4] → department[0].embedding, department[0].teams[0-3].embedding, department[0].projects[0].embedding
 * - embeddings[1][0-4] → department[1].embedding, department[1].teams[0-3].embedding, department[1].projects[0].embedding
 * - etc.
 */
public class HierarchicalVector implements DocTemplate {

    // Vector configuration
    private static final int VECTOR_DIMENSION = 128;
    private static final int OUTER_ARRAY_SIZE = 4;   // 4 arrays in embeddings
    private static final int INNER_ARRAY_SIZE = 5;   // 5 vectors per inner array
    private static final int VECTORS_PER_DOC = OUTER_ARRAY_SIZE * INNER_ARRAY_SIZE; // 20 vectors

    // For shared vectors across documents
    private static final int VECTOR_POOL_SIZE = 100;

    private static final List<String> DEPARTMENTS = Arrays.asList(
        "Engineering", "Sales", "Marketing", "HR"
    );

    private static final List<String> TEAM_TYPES = Arrays.asList(
        "Backend", "Frontend", "DevOps", "QA", "Data"
    );

    private static final List<String> FIRST_NAMES = Arrays.asList(
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi"
    );

    @Override
    public JsonObject createJsonObject(Faker faker, int docsize, int docId) {
        JsonObject doc = JsonObject.create();

        // Document metadata
        doc.put("doc_id", docId);
        doc.put("type", "hierarchical_vector");

        // Pre-generate all 20 vectors for this document
        JsonArray[][] vectors = generateAllVectors(docId);

        // ROOT LEVEL: embeddings - array of arrays (ground truth)
        JsonArray embeddingsArray = JsonArray.create();
        for (int outer = 0; outer < OUTER_ARRAY_SIZE; outer++) {
            JsonArray innerArray = JsonArray.create();
            for (int inner = 0; inner < INNER_ARRAY_SIZE; inner++) {
                innerArray.add(vectors[outer][inner]);
            }
            embeddingsArray.add(innerArray);
        }
        doc.put("embeddings", embeddingsArray);

        // NESTED STRUCTURE: company hierarchy with same vectors spread out
        doc.put("company", createCompanyHierarchy(faker, docId, vectors));

        return doc;
    }

    @Override
    public JsonObject updateJsonObject(Faker faker, JsonObject obj, List<String> fieldsToUpdate) {
        int docId = obj.getInt("doc_id");
        JsonArray[][] vectors = generateAllVectors(docId);

        if (fieldsToUpdate != null && !fieldsToUpdate.isEmpty() && !fieldsToUpdate.get(0).isEmpty()) {
            for (String field : fieldsToUpdate) {
                if ("company".equals(field)) {
                    obj.put("company", createCompanyHierarchy(faker, docId, vectors));
                }
            }
        }
        return obj;
    }

    /**
     * Pre-generates all 20 vectors for a document
     */
    private JsonArray[][] generateAllVectors(int docId) {
        JsonArray[][] vectors = new JsonArray[OUTER_ARRAY_SIZE][INNER_ARRAY_SIZE];

        for (int outer = 0; outer < OUTER_ARRAY_SIZE; outer++) {
            for (int inner = 0; inner < INNER_ARRAY_SIZE; inner++) {
                vectors[outer][inner] = generateVector(docId, outer, inner);
            }
        }

        return vectors;
    }

    /**
     * Creates company hierarchy with vectors spread throughout
     *
     * Vector distribution (20 vectors across 4 departments):
     * - Each department gets 5 vectors from embeddings[dept_idx][0-4]
     *   - dept.embedding = embeddings[dept_idx][0]
     *   - dept.teams[0].embedding = embeddings[dept_idx][1]
     *   - dept.teams[1].embedding = embeddings[dept_idx][2]
     *   - dept.teams[2].embedding = embeddings[dept_idx][3]
     *   - dept.projects[0].embedding = embeddings[dept_idx][4]
     */
    private JsonObject createCompanyHierarchy(Faker faker, int docId, JsonArray[][] vectors) {
        JsonObject company = JsonObject.create();
        company.put("id", "c" + docId);
        company.put("name", faker.company().name());

        // Create 4 departments, each with vectors from embeddings[dept_idx]
        JsonArray departments = JsonArray.create();

        for (int deptIdx = 0; deptIdx < OUTER_ARRAY_SIZE; deptIdx++) {
            JsonObject dept = JsonObject.create();
            dept.put("dept_id", deptIdx);
            dept.put("name", DEPARTMENTS.get(deptIdx));
            dept.put("budget", faker.number().numberBetween(500000, 2000000));

            // Department embedding: embeddings[deptIdx][0]
            dept.put("embedding", vectors[deptIdx][0]);

            // Teams array with embeddings: embeddings[deptIdx][1], [2], [3]
            JsonArray teams = JsonArray.create();
            for (int teamIdx = 0; teamIdx < 3; teamIdx++) {
                JsonObject team = JsonObject.create();
                team.put("team_id", teamIdx);
                team.put("name", TEAM_TYPES.get(teamIdx) + " Team");
                team.put("team_lead", FIRST_NAMES.get((docId + deptIdx + teamIdx) % FIRST_NAMES.size()));

                // Team embedding: embeddings[deptIdx][1 + teamIdx]
                team.put("embedding", vectors[deptIdx][1 + teamIdx]);

                // Add some members (no embeddings, just for hierarchy depth)
                JsonArray members = JsonArray.create();
                for (int m = 0; m < 2; m++) {
                    JsonObject member = JsonObject.create();
                    member.put("name", FIRST_NAMES.get((docId + m) % FIRST_NAMES.size()));
                    member.put("role", teamIdx == 0 ? "Engineer" : "Analyst");
                    members.add(member);
                }
                team.put("members", members);

                teams.add(team);
            }
            dept.put("teams", teams);

            // Projects array with embedding: embeddings[deptIdx][4]
            JsonArray projects = JsonArray.create();
            JsonObject project = JsonObject.create();
            project.put("project_id", 0);
            project.put("title", "Project " + DEPARTMENTS.get(deptIdx).charAt(0));
            project.put("status", "ongoing");
            // Project embedding: embeddings[deptIdx][4]
            project.put("embedding", vectors[deptIdx][4]);
            projects.add(project);
            dept.put("projects", projects);

            departments.add(dept);
        }

        company.put("departments", departments);

        return company;
    }

    /**
     * Generates a deterministic 128-dimensional vector based on position.
     *
     * Sharing logic:
     * - 70% of vectors are unique to the document
     * - 30% of vectors are shared across documents (based on position only)
     */
    private JsonArray generateVector(int docId, int outerIdx, int innerIdx) {
        long seed;

        int positionHash = outerIdx * INNER_ARRAY_SIZE + innerIdx;
        boolean isShared = (positionHash % 10) < 3; // 30% chance of being shared

        if (isShared) {
            // Shared vector: seed based only on position (not doc_id)
            seed = (long) outerIdx * 10000 + innerIdx * 100 + (docId % VECTOR_POOL_SIZE);
        } else {
            // Unique vector: seed based on doc_id AND position
            seed = (long) docId * 1000000 + outerIdx * 10000 + innerIdx * 100;
        }

        Random vectorRandom = new Random(seed);
        JsonArray embedding = JsonArray.create();

        for (int d = 0; d < VECTOR_DIMENSION; d++) {
            double value = (vectorRandom.nextDouble() * 2) - 1;
            value = Math.round(value * 1000000.0) / 1000000.0;
            embedding.add(value);
        }

        return embedding;
    }

    /**
     * Utility method to regenerate a specific vector for ground truth validation.
     */
    public static double[] getExpectedVector(int docId, int outerIdx, int innerIdx) {
        long seed;

        int positionHash = outerIdx * INNER_ARRAY_SIZE + innerIdx;
        boolean isShared = (positionHash % 10) < 3;

        if (isShared) {
            seed = (long) outerIdx * 10000 + innerIdx * 100 + (docId % VECTOR_POOL_SIZE);
        } else {
            seed = (long) docId * 1000000 + outerIdx * 10000 + innerIdx * 100;
        }

        Random vectorRandom = new Random(seed);
        double[] vector = new double[VECTOR_DIMENSION];

        for (int d = 0; d < VECTOR_DIMENSION; d++) {
            double value = (vectorRandom.nextDouble() * 2) - 1;
            vector[d] = Math.round(value * 1000000.0) / 1000000.0;
        }

        return vector;
    }

    public static boolean isSharedVector(int outerIdx, int innerIdx) {
        int positionHash = outerIdx * INNER_ARRAY_SIZE + innerIdx;
        return (positionHash % 10) < 3;
    }

    public static int getVectorDimension() { return VECTOR_DIMENSION; }
    public static int getOuterArraySize() { return OUTER_ARRAY_SIZE; }
    public static int getInnerArraySize() { return INNER_ARRAY_SIZE; }
    public static int getVectorsPerDoc() { return VECTORS_PER_DOC; }
}
