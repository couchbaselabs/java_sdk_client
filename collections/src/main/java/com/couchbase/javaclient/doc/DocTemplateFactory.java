package com.couchbase.javaclient.doc;


public final class DocTemplateFactory {

	public static DocTemplate getDocTemplate(DocSpec ds) {
		if ("emp".equals(ds.get_template())) {
			return new Emp();
		}else if("Employee".equals(ds.get_template())){
			return new Employee();
		}else  if("Person".equals(ds.get_template())){
			return new Person();
		}else  if("Hotel".equals(ds.get_template())){
			return new Hotel();
		}else  if("hierarchical".equals(ds.get_template()) || "Hierarchical".equals(ds.get_template())){
			return new Hierarchical();
		}else  if("hierarchical_vector".equals(ds.get_template()) || "HierarchicalVector".equals(ds.get_template())){
			return new HierarchicalVector();
		}else {
			return new TextDataSet(ds);
		}
	}

}

