package com.couchbase.javaclient.doc;

import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.ArrayList;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.time.format.DateTimeFormatter;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

//{
//    "address": "Capstone Road, ME7 3JE",
//    "city": "Medway",
//    "country": "United Kingdom",
//    "email": null, -> can be an optional field
//    "free_breakfast": true,
//    "free_parking": true,
//    "name": "Medway Youth Hostel",
//    "phone": "+44 870 770 5964",
//    "price": 1000, -> integer or float
//    "avg_rating" : 3.5, -> integer or float
//    "public_likes": [ -> scalar array of names, number of names in list could be between 0 & 10 ? maybe we can have just first names here to reduce doc size ?
//      "Julius Tromp I",
//      "Corrine Hilll",
//      "Jaeden McKenzie",
//      "Vallie Ryan",
//      "Brian Kilback",
//      "Lilian McLaughlin",
//      "Ms. Moses Feeney",
//      "Elnora Trantow"
//    ],
//    "reviews": [
//      {
//        "author": "Ozella Sipes",
//        "date": "2013-06-22 18:33:50 +0300",
//        "ratings": { -> could have other fields as well, but can be missing for some.
//          "Cleanliness": 5,
//          "Overall": 4,
//          "Value": 4
//        }
//      },
//      {
//        "author": "Barton Marks",
//        "date": "2015-03-02 19:56:13 +0300",
//        "ratings": {
//          "Check in / front desk": 4,
//          "Cleanliness": 4,
//          "Overall": 4,
//          "Rooms": 3,
//          "Value": 5
//        }
//      }
//    ],
//    "type": "hotel", -> this we can remove if needed
//    "url": "http://www.yha.org.uk",
//  }
//}

public class Hotel implements DocTemplate {
	JsonObject jsonObject = JsonObject.create();

	public JsonObject createJsonObject(Faker faker, int docsize, int id) {
		HotelDetails hdetails = new HotelDetails(faker);
		jsonObject.put("address", faker.address().streetAddress());
		jsonObject.put("city", faker.address().city());
		jsonObject.put("country", faker.address().country());
		jsonObject.put("email", hdetails.getEmail());
		jsonObject.put("free_breakfast", ThreadLocalRandom.current().nextBoolean());
		jsonObject.put("free_parking", ThreadLocalRandom.current().nextBoolean());
		jsonObject.put("name", hdetails.getName());
		jsonObject.put("phone", faker.phoneNumber().phoneNumber());
		jsonObject.put("price", (float) ThreadLocalRandom.current().nextInt(500, 2000));
		jsonObject.put("avg_rating", (float) ThreadLocalRandom.current().nextInt(1, 5));
		jsonObject.put("public_likes", hdetails.getLikesArray());
		jsonObject.put("reviews", hdetails.getReviewsArray());
		jsonObject.put("type", hdetails.getType());
		jsonObject.put("url", faker.internet().url());
		return jsonObject;
	}

	public JsonObject updateJsonObject(Faker faker, JsonObject obj, List<String> fieldsToUpdate) {		
		HotelDetails hdetails = new HotelDetails(faker);
		if(fieldsToUpdate.contains("address")){
			obj.put("address", faker.address().streetAddress());
		}
		if(fieldsToUpdate.contains("city")){
			obj.put("city", faker.address().city());
		}
		if(fieldsToUpdate.contains("country")){
			obj.put("country", faker.address().country());
		}
		if(fieldsToUpdate.contains("email")){
			obj.put("email", hdetails.getEmail());
		}
		if(fieldsToUpdate.contains("free_breakfast")){
			obj.put("free_breakfast", ThreadLocalRandom.current().nextBoolean());
		}
		if(fieldsToUpdate.contains("free_parking")){
			obj.put("free_parking", ThreadLocalRandom.current().nextBoolean());
		}
		if(fieldsToUpdate.contains("name")){
			obj.put("name", hdetails.getName());
		}
		if(fieldsToUpdate.contains("phone")){
			obj.put("phone", faker.phoneNumber().phoneNumber());
		}
		if(fieldsToUpdate.contains("price")){
			obj.put("price", (float) ThreadLocalRandom.current().nextInt(500, 2000));
		}
		if(fieldsToUpdate.contains("avg_rating")){
			obj.put("avg_rating", (float) ThreadLocalRandom.current().nextInt(1, 5));
		}
		if(fieldsToUpdate.contains("public_likes")){
			obj.put("public_likes", hdetails.getLikesArray());
		}
		if(fieldsToUpdate.contains("reviews")){
			obj.put("reviews", hdetails.getReviewsArray());
		}
		if(fieldsToUpdate.contains("type")){
			obj.put("type", hdetails.getType());
		}
		if(fieldsToUpdate.contains("url")){
			obj.put("url", faker.internet().url());
		}
		return obj;
	}

	private class HotelDetails {
		private String name;
		private Object email;
		private String firstName;
		private String lastName;
		private List<String> htypes = Arrays.asList("Inn", "Hostel", "Place", "Center", "Hotel", "Motel", "Suites");
		private List<Object> emails = new ArrayList<Object>();
		private List<Object> likes = new ArrayList<Object>();
		private List<Object> reviews = new ArrayList<Object>();
		private String type;
		private Faker faker;

		public HotelDetails(Faker faker) {
			this.faker = faker;
			this.firstName = faker.name().firstName();
			this.lastName = faker.name().lastName();
			this.setType();
			this.setName();
			this.setEmail();
			this.setLikesArray();
			this.setReviewsArray();
		}

		public void setName() {
			this.name = firstName + ' ' + lastName + ' ' + type;
		}

		public void setType() {
			int rindex = ThreadLocalRandom.current().nextInt(htypes.size());
			this.type = htypes.get(rindex);
		}

		public void setEmail() {
			emails.add(null);
			emails.add(firstName + '.' + lastName + "@hotels.com");
			emails.add(lastName + '.' + firstName + "@hotels.com");
			int rindex = ThreadLocalRandom.current().nextInt(emails.size());
			this.email = emails.get(rindex);
		}

		public void setLikesArray() {
			int numLikes = ThreadLocalRandom.current().nextInt(0, 10);
			for (int n = 0; n <= numLikes; n++) {
				this.likes.add(faker.name().fullName());
			}
		}

		public void setReviewsArray() {
			int numReviews = ThreadLocalRandom.current().nextInt(0, 10);
			LocalDateTime now = LocalDateTime.now();
			for (int n = 0; n <= numReviews; n++) {
				JsonObject review = JsonObject.create();
				review.put("author", faker.name().fullName());
				review.put("date", now.plus(n, ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
				JsonObject ratings = JsonObject.create();
				ratings.put("Check in / front desk", ThreadLocalRandom.current().nextInt(1, 5));
				ratings.put("Cleanliness", ThreadLocalRandom.current().nextInt(1, 5));
				ratings.put("Overall", ThreadLocalRandom.current().nextInt(1, 5));
				ratings.put("Rooms", ThreadLocalRandom.current().nextInt(1, 5));
				ratings.put("Value", ThreadLocalRandom.current().nextInt(1, 5));
				review.put("ratings", ratings);
				this.reviews.add(review);
			}
		}

		private List<Object> getReviewsArray() {
			return reviews;
		}

		public String getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public Object getEmail() {
			return email;
		}

		public List<Object> getLikesArray() {
			return likes;
		}
	}

}
