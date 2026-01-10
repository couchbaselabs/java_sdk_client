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
 * - Root level "embeddings": SINGLE flat array with 20 vectors (ground truth)
 * - Nested hierarchy: Same 20 vectors spread throughout company → departments → teams
 *
 * This allows:
 * - Ground truth access via: doc.embeddings[0], doc.embeddings[1], ... doc.embeddings[19]
 * - Search validation via nested paths: doc.company.departments[i].teams[j].embedding
 *
 * Vector distribution in hierarchy:
 * - embeddings[0-4]   → department[0]: dept.embedding + 3 teams + 1 project
 * - embeddings[5-9]   → department[1]: dept.embedding + 3 teams + 1 project
 * - embeddings[10-14] → department[2]: dept.embedding + 3 teams + 1 project
 * - embeddings[15-19] → department[3]: dept.embedding + 3 teams + 1 project
 */
public class HierarchicalVector implements DocTemplate {

    // Vector configuration
    private static final int VECTOR_DIMENSION = 128;
    private static final int VECTORS_PER_DOC = 20;  // Fixed 20 vectors per document
    private static final int NUM_DEPARTMENTS = 4;   // 4 departments
    private static final int VECTORS_PER_DEPT = 5;  // 5 vectors per department

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

        // Pre-generate all 20 vectors for this document as a FLAT array
        JsonArray[] vectors = generateAllVectors(docId);

        // ROOT LEVEL: embeddings - SINGLE flat array with 20 vectors (ground truth)
        JsonArray embeddingsArray = JsonArray.create();
        for (int i = 0; i < VECTORS_PER_DOC; i++) {
            embeddingsArray.add(vectors[i]);
        }
        doc.put("embeddings", embeddingsArray);

        // NESTED STRUCTURE: company hierarchy with same vectors spread out
        doc.put("company", createCompanyHierarchy(faker, docId, vectors));

        return doc;
    }

    @Override
    public JsonObject updateJsonObject(Faker faker, JsonObject obj, List<String> fieldsToUpdate) {
        int docId = obj.getInt("doc_id");
        JsonArray[] vectors = generateAllVectors(docId);

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
     * Pre-generates all 20 vectors for a document as a flat array
     */
    private JsonArray[] generateAllVectors(int docId) {
        JsonArray[] vectors = new JsonArray[VECTORS_PER_DOC];

        for (int i = 0; i < VECTORS_PER_DOC; i++) {
            vectors[i] = generateVector(docId, i);
        }

        return vectors;
    }

    /**
     * Creates company hierarchy with vectors spread throughout
     *
     * Vector distribution (20 vectors across 4 departments, 5 each):
     * - Department 0: embeddings[0] (dept) + embeddings[1-3] (teams) + embeddings[4] (project)
     * - Department 1: embeddings[5] (dept) + embeddings[6-8] (teams) + embeddings[9] (project)
     * - Department 2: embeddings[10] (dept) + embeddings[11-13] (teams) + embeddings[14] (project)
     * - Department 3: embeddings[15] (dept) + embeddings[16-18] (teams) + embeddings[19] (project)
     */
    private JsonObject createCompanyHierarchy(Faker faker, int docId, JsonArray[] vectors) {
        JsonObject company = JsonObject.create();
        company.put("id", "c" + docId);
        company.put("name", faker.company().name());

        // Create 4 departments, each with 5 vectors
        JsonArray departments = JsonArray.create();

        for (int deptIdx = 0; deptIdx < NUM_DEPARTMENTS; deptIdx++) {
            int baseVectorIdx = deptIdx * VECTORS_PER_DEPT;  // 0, 5, 10, 15

            JsonObject dept = JsonObject.create();
            dept.put("dept_id", deptIdx);
            dept.put("name", DEPARTMENTS.get(deptIdx));
            dept.put("budget", faker.number().numberBetween(500000, 2000000));

            // Department embedding: embeddings[baseVectorIdx] (0, 5, 10, 15)
            dept.put("embedding", vectors[baseVectorIdx]);

            // Teams array with embeddings: embeddings[baseVectorIdx + 1, 2, 3]
            JsonArray teams = JsonArray.create();
            for (int teamIdx = 0; teamIdx < 3; teamIdx++) {
                JsonObject team = JsonObject.create();
                team.put("team_id", teamIdx);
                team.put("name", TEAM_TYPES.get(teamIdx) + " Team");
                team.put("team_lead", FIRST_NAMES.get((docId + deptIdx + teamIdx) % FIRST_NAMES.size()));

                // Team embedding: embeddings[baseVectorIdx + 1 + teamIdx]
                team.put("embedding", vectors[baseVectorIdx + 1 + teamIdx]);

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

            // Projects array with embedding: embeddings[baseVectorIdx + 4]
            JsonArray projects = JsonArray.create();
            JsonObject project = JsonObject.create();
            project.put("project_id", 0);
            project.put("title", "Project " + DEPARTMENTS.get(deptIdx).charAt(0));
            project.put("status", "ongoing");
            // Project embedding: embeddings[baseVectorIdx + 4] (4, 9, 14, 19)
            project.put("embedding", vectors[baseVectorIdx + 4]);
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
    private JsonArray generateVector(int docId, int vectorIdx) {
        long seed;

        boolean isShared = (vectorIdx % 10) < 3; // 30% chance of being shared

        if (isShared) {
            // Shared vector: seed based only on position (not doc_id)
            seed = (long) vectorIdx * 10000 + (docId % VECTOR_POOL_SIZE);
        } else {
            // Unique vector: seed based on doc_id AND position
            seed = (long) docId * 1000000 + vectorIdx * 100;
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
     *
     * @param docId Document ID
     * @param vectorIdx Vector index (0-19)
     * @return The expected 128-dimensional vector as double array
     */
    public static double[] getExpectedVector(int docId, int vectorIdx) {
        long seed;

        boolean isShared = (vectorIdx % 10) < 3;

        if (isShared) {
            seed = (long) vectorIdx * 10000 + (docId % VECTOR_POOL_SIZE);
        } else {
            seed = (long) docId * 1000000 + vectorIdx * 100;
        }

        Random vectorRandom = new Random(seed);
        double[] vector = new double[VECTOR_DIMENSION];

        for (int d = 0; d < VECTOR_DIMENSION; d++) {
            double value = (vectorRandom.nextDouble() * 2) - 1;
            vector[d] = Math.round(value * 1000000.0) / 1000000.0;
        }

        return vector;
    }

    public static boolean isSharedVector(int vectorIdx) {
        return (vectorIdx % 10) < 3;
    }

    public static int getVectorDimension() { return VECTOR_DIMENSION; }
    public static int getVectorsPerDoc() { return VECTORS_PER_DOC; }
    public static int getNumDepartments() { return NUM_DEPARTMENTS; }
    public static int getVectorsPerDept() { return VECTORS_PER_DEPT; }
}
