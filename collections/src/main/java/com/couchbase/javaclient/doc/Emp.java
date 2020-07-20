package com.couchbase.javaclient.doc;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Date;
import java.util.Calendar;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.github.javafaker.Faker;

public class Emp implements DocTemplate{

    JsonObject jsonObject = JsonObject.create();
    private static final String[] FIRST_NAMES = {"Adara", "Adena", "Adrianne", "Alarice", "Alvita", "Amara",
            "Ambika", "Antonia", "Araceli", "Balandria", "Basha",
            "Beryl", "Bryn", "Callia", "Caryssa", "Cassandra", "Casondrah",
            "Chatha", "Ciara", "Cynara", "Cytheria", "Dabria", "Darcei",
            "Deandra", "Deirdre", "Delores", "Desdomna", "Devi", "Dominique",
            "Drucilla", "Duvessa", "Ebony", "Fantine", "Fuscienne",
            "Gabi", "Gallia", "Hanna", "Hedda", "Jerica", "Jetta", "Joby",
            "Kacila", "Kagami", "Kala", "Kallie", "Keelia", "Kerry",
            "Kerry-Ann", "Kimberly", "Killian", "Kory", "Lilith",
            "Lucretia", "Lysha", "Mercedes", "Mia", "Maura", "Perdita",
            "Quella","Riona", "Safiya", "Salina", "Severin", "Sidonia",
            "Sirena", "Solita", "Tempest", "Thea", "Treva", "Trista",
            "Vala", "Winta"};
    private static final String[] LAST_NAMES = {"Smith", "Brown", "Johnson", "Jones", "Williams",
            "Davis", "Miller XII", "Wilson", "Taylor", "Clark", "White",
            "Moore", "Thompson", "Allen", "Martin", "Hall", "Adams",
            "Thomas", "Wright", "Baker III", "Walker II", "Anderson", "Lewis",
            "Harris", "Hill", "King Jr.", "Jackson", "Lee", "Green", "Wood",
            "Parker X", "Campbell", "Young", "Robinson Sr.", "Stewart",
            "Scott", "Rogers", "Roberts", "Cook", "Phillips", "Turner",
            "Carter", "Ward", "Foster", "Morgan", "Howard Jr.", "Cox",
            "Bailey", "Richardson IX", "Reed", "Russell", "Edwards Sr.",
            "Cooper", "Wells", "Palmer", "Ann", "Mitchell", "Evans",
            "Simón", "Josué", "Damián", "Julián", "Aarón", "Ángel",
            "Gerónimo", "Juan José", "Tomás", "Nicolás", "Sebastián",
            "Jr.", "Sr."};
    private static final String[] DEPT = {"Engineering", "Sales", "Support", "Marketing", "Info-tech", "Finance",
            "HR", "Pre-sales", "Accounts", "Dev-ops", "Training"};
    private final static String[] LANGUAGES = {"English", "Spanish", "German", "Italian", "French",
            "Arabic", "Africans", "Hindi", "Vietnamese", "Urdu", "Dutch",
            "Quechua", "Japanese", "Chinese", "Nepalese", "Thai", "Malay",
            "Sinhalese", "Portuguese", "Romanian"};
    Random random = new Random();

    public JsonObject createJsonObject(Faker faker, int docsize, int id) {
        String empName = generateName();
        boolean isManager = random.nextBoolean();
        jsonObject.put("name", empName);
        jsonObject.put("emp_id", ""+(10000000+id));
        jsonObject.put("dept", generateDept());
        jsonObject.put("email", empName.split(" ")[0] + "@mcdiabetes.com");
        jsonObject.put("salary", generateSalary());
        jsonObject.put("join_date", generateJoinDate());
        jsonObject.put("languages_known", generateLangKnown());
        jsonObject.put("is_manager", isManager);
        jsonObject.put("mutated", 0);
        jsonObject.put("type", "emp");
        if(isManager){
            JsonObject manages = JsonObject.create();
            int teamSize = 5 + random.nextInt(5);
            manages.put("team_size", teamSize);
            List<String> reports = new ArrayList<>();
            for(int i=0; i<teamSize; i++){
                reports.add(generateName());
            }
            manages.put("reports", reports);
            jsonObject.put("manages", manages);
        }

        return jsonObject;
    }

    public JsonObject updateJsonObject(JsonObject obj, List<String> fieldsToUpdate) {
        if(fieldsToUpdate == null || fieldsToUpdate.size() == 0){
            obj.put("salary", generateSalary());
            obj.put("mutated", 1);
            return obj;
        }
        if(fieldsToUpdate.contains("salary")){
            obj.put("salary", generateSalary());
        }
        if(fieldsToUpdate.contains("dept")){
            obj.put("dept", generateDept());
        }
        if(fieldsToUpdate.contains("is_manager")){
            obj.put("is_manager", random.nextBoolean());
        }
        if(obj.getBoolean("is_manager")){
            if(fieldsToUpdate.contains("manages.team_size") || fieldsToUpdate.contains("manages.reports")){
                JsonObject manages = JsonObject.create();
                int teamSize = 5 + random.nextInt(5);
                manages.put("team_size", teamSize);
                List<String> reports = new ArrayList<>();
                for(int i=0; i<teamSize; i++){
                    reports.add(generateName());
                }
                manages.put("reports", reports);
                obj.put("manages", manages);

            }
        }
        if(fieldsToUpdate.contains("languages_known")){
            obj.put("languages_known", generateLangKnown());
        }
        if(fieldsToUpdate.contains("email")){
            obj.put("email", generateName().split(" ")[0] + "@mcdiabetes.com");
        }
        obj.put("mutated", 1);
        return obj;
    }


    private String generateName(){
        String res = "";
        res +=FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        res += " ";
        res+=LAST_NAMES[random.nextInt(LAST_NAMES.length)];
        return res;
    }

    private String generateDept(){
        return DEPT[random.nextInt(DEPT.length)];
    }

    private int generateSalary(){
        return (random.nextInt(100000) + 50000);
    }

    private String generateJoinDate(){
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1950 + random.nextInt(2016-1950 + 1));
        cal.set(Calendar.MONTH, 1 + random.nextInt(11));
        cal.set(Calendar.DAY_OF_MONTH, 1 + random.nextInt(27));
        cal.set(Calendar.HOUR_OF_DAY, random.nextInt(23));
        cal.set(Calendar.MINUTE, 1 + random.nextInt(58));
        Date date = cal.getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

        return dateFormat.format(date)+"T"+timeFormat.format(date);
    }

    private List<String> generateLangKnown(){
        int count = 0;
        List<String> lang = new ArrayList<>();
        while (count < 3){
            lang.add(LANGUAGES[random.nextInt(LANGUAGES.length)]);
            count++;
        }
        return lang;

    }
}
