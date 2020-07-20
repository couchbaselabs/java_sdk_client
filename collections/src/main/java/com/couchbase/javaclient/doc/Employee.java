package com.couchbase.javaclient.doc;

import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

public class Employee implements DocTemplate{
	JsonObject jsonObject = JsonObject.create();
	Random random = new Random();
	Calendar calendar = new GregorianCalendar();
	List<String> jobTitles = Arrays.asList("Engineer", "Sales", "Support");
	List<Integer> joinYears = Arrays.asList(2010, 2011);
	Map<String, String> ubuntu = new HashMap<>();
	Map<String, String> windows = new HashMap<>();
	Map<String, Integer> task = new HashMap<>();

	/*
	 * template: { "name": "employee-5", "join_yr": 2010, "join_mo": 8, "join_day":
	 * 16, "email": "16-mail@couchbase.com", "job_title": "Engineer", "test_rate":
	 * 8.08, "skills": [ "skill2010", "skill2011" ], "VMs": [ { "name": "vm_8",
	 * "memory": "8", "os": "ubuntu", "RAM": "8" }, { "name": "vm_9", "memory": "8",
	 * "os": "windows", "RAM": "8" } ], "tasks_points": { "task1": 0, "task2": 1 } }
	 */

	public JsonObject createJsonObject(Faker faker, int docsize, int id) {
		Date joinDate = faker.date().past(365*10, TimeUnit.DAYS);
		calendar.setTime(joinDate);
		int join_day = calendar.get(Calendar.DAY_OF_MONTH);
		int join_month = calendar.get(Calendar.MONTH) + 1;
		jsonObject.put("name", "employee-" + joinDate.getDay());
		jsonObject.put("join_yr", joinYears.get(random.nextInt(joinYears.size())));
		jsonObject.put("join_mo", join_month);
		jsonObject.put("join_day", join_day);
		jsonObject.put("email", Integer.toString(join_day) + "-mail@couchbase.com");
		jsonObject.put("job_title", jobTitles.get(random.nextInt(jobTitles.size())));
		jsonObject.put("test_rate", (float) join_month + (join_month * 0.01));
		jsonObject.put("skills", this.getSkillsArray());
		jsonObject.put("VMs", this.getVMsArray(join_month));
		jsonObject.put("tasks_points", this.getTaskPoints());
		return jsonObject;
	}


	public Object updateJsonObject(JsonObject obj, List<String> fieldsToUpdate) {
		return obj;
	}


	private List<String> getSkillsArray() {
		return Arrays.asList("skill2010", "skill2011");
	}

	private List<Map<String, String>> getVMsArray(int month) {
		String next_month = Integer.toString(month + 1);
		String this_month = Integer.toString(month);
		ubuntu.put("RAM", this_month);
		ubuntu.put("os", "ubuntu");
		ubuntu.put("name", "vm_" + this_month);
		ubuntu.put("memory", this_month);
		windows.put("RAM", this_month);
		windows.put("os", "windows");
		windows.put("name", "vm_" + next_month);
		windows.put("memory", this_month);
		return Arrays.asList(ubuntu, windows);
	}

	private Map<String, Integer> getTaskPoints() {
		task.put("task1", 0);
		task.put("task2", 1);
		return task;
	}

}
