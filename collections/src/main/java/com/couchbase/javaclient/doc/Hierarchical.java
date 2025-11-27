package com.couchbase.javaclient.doc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonArray;
import com.github.javafaker.Faker;

/**
 * Hierarchical document template for testing hierarchical search functionality.
 * Creates nested JSON structures matching the pattern:
 * - Company
 *   - Departments (Array)
 *     - Employees (Array - can be nested arrays)
 *     - Projects (Array)
 *   - Locations (Array)
 *
 * Supports:
 * - Nested arrays (employees as array of array of array)
 * - Variable fields (role, home, or both)
 * - Large arrays for stress testing
 * - Duplicate values for realistic search scenarios
 */
public class Hierarchical implements DocTemplate {

    private Random random = new Random();

    private static final List<String> DEPARTMENTS = Arrays.asList(
        "Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Support"
    );

    private static final List<String> ROLES = Arrays.asList(
        "Engineer", "Manager", "Salesperson", "Marketer", "HR", "Support Engineer", "Intern", "Analyst"
    );

    private static final List<String> PROJECT_STATUSES = Arrays.asList(
        "ongoing", "completed", "planned"
    );

    private static final List<String> FIRST_NAMES = Arrays.asList(
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Mallory"
    );

    private static final List<String> CITIES = Arrays.asList(
        "Athens", "Berlin", "London", "Paris", "Tokyo", "New York", "Chennai", "Edinburgh"
    );

    private static final List<String> COUNTRIES = Arrays.asList(
        "USA", "UK", "Greece", "Germany", "France", "Japan", "India", "Canada"
    );

    @Override
    public JsonObject createJsonObject(Faker faker, int docsize, int id) {
        JsonObject doc = JsonObject.create();

        if (random.nextBoolean()) {
            doc.put("id", "doc" + id);
        }

        doc.put("company", createCompany(faker, id));

        int currentSize = doc.toString().length();
        if (currentSize < docsize) {
            doc.put("_filler", createFillerContent(faker, docsize - currentSize));
        }

        return doc;
    }

    @Override
    public JsonObject updateJsonObject(Faker faker, JsonObject obj, List<String> fieldsToUpdate) {
        if (fieldsToUpdate != null && !fieldsToUpdate.isEmpty() && !fieldsToUpdate.get(0).isEmpty()) {
            for (String field : fieldsToUpdate) {
                if ("company".equals(field)) {
                    obj.put("company", createCompany(faker, random.nextInt(10000)));
                }
            }
        } else {
            obj.put("company", createCompany(faker, random.nextInt(10000)));
        }
        return obj;
    }

    private JsonObject createCompany(Faker faker, int id) {
        JsonObject company = JsonObject.create();
        company.put("id", "c" + id);
        company.put("name", faker.company().name());

        JsonArray departments = JsonArray.create();
        int numDepts = ThreadLocalRandom.current().nextInt(1, 4);
        for (int i = 0; i < numDepts; i++) {
            departments.add(createDepartment(faker));
        }
        company.put("departments", departments);

        JsonArray locations = JsonArray.create();
        int numLocations = ThreadLocalRandom.current().nextInt(1, 4);
        for (int i = 0; i < numLocations; i++) {
            locations.add(createLocation(faker));
        }
        company.put("locations", locations);

        return company;
    }

    private JsonObject createDepartment(Faker faker) {
        JsonObject dept = JsonObject.create();
        dept.put("name", DEPARTMENTS.get(random.nextInt(DEPARTMENTS.size())));
        dept.put("budget", faker.number().numberBetween(200000, 2000000));

        dept.put("employees", createEmployeesArray(faker));

        JsonArray projects = JsonArray.create();
        int numProjects = ThreadLocalRandom.current().nextInt(2, 10);
        for (int i = 0; i < numProjects; i++) {
            projects.add(createProject(faker));
        }
        dept.put("projects", projects);

        return dept;
    }

    private JsonArray createEmployeesArray(Faker faker) {
        boolean createNestedArrays = random.nextInt(100) < 30;

        if (createNestedArrays) {
            JsonArray level1 = JsonArray.create();
            JsonArray level2 = JsonArray.create();
            JsonArray level3 = JsonArray.create();

            int numEmployees = ThreadLocalRandom.current().nextInt(20, 40);
            for (int i = 0; i < numEmployees; i++) {
                level3.add(createEmployee(faker));
            }

            level2.add(level3);
            level1.add(level2);
            return level1;
        } else {
            JsonArray employees = JsonArray.create();
            int numEmployees = ThreadLocalRandom.current().nextInt(2, 6);
            for (int i = 0; i < numEmployees; i++) {
                employees.add(createEmployee(faker));
            }
            return employees;
        }
    }

    private JsonObject createEmployee(Faker faker) {
        JsonObject employee = JsonObject.create();

        employee.put("name", FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size())));

        int fieldPattern = random.nextInt(4);
        if (fieldPattern == 0) {
            employee.put("role", ROLES.get(random.nextInt(ROLES.size())));
        } else if (fieldPattern == 1) {
            employee.put("home", CITIES.get(random.nextInt(CITIES.size())));
        } else {
            employee.put("role", ROLES.get(random.nextInt(ROLES.size())));
            employee.put("home", CITIES.get(random.nextInt(CITIES.size())));
        }

        return employee;
    }

    private JsonObject createProject(Faker faker) {
        JsonObject project = JsonObject.create();
        project.put("title", "Project " + faker.lorem().word().substring(0, 1).toUpperCase() + faker.lorem().word().substring(1));
        project.put("status", PROJECT_STATUSES.get(random.nextInt(PROJECT_STATUSES.size())));
        return project;
    }

    private JsonObject createLocation(Faker faker) {
        JsonObject location = JsonObject.create();
        location.put("city", CITIES.get(random.nextInt(CITIES.size())));
        location.put("country", COUNTRIES.get(random.nextInt(COUNTRIES.size())));
        return location;
    }


    private JsonObject createFillerContent(Faker faker, int targetSize) {
        JsonObject filler = JsonObject.create();
        int count = 0;
        while (filler.toString().length() < targetSize) {
            count++;
            filler.put("filler_" + count, faker.lorem().paragraph());
        }
        return filler;
    }
}

