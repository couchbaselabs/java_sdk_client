package com.couchbase.javaclient.doc;


public final class DocTemplateFactory {

	public static DocTemplate getDocTemplate(final String dataset, String path, int numOps) {
		if ("emp".equals(dataset)) {
			return new Emp();
		}else if("Employee".equals(dataset)){
			return new Employee();
		}else  if("Person".equals(dataset)){
			return new Person();
		}else if ("napa".equals(dataset)) {
			return new Napa(path, numOps);
		}else if("wiki".equals(dataset)){
			return new Wiki(path, numOps);
		}else {
			throw new RuntimeException("Unknown dataset - " + dataset);
		}
	}

}

