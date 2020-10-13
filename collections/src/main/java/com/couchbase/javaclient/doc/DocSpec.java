package com.couchbase.javaclient.doc;

import com.github.javafaker.Faker;

public class DocSpec {
	int _num_ops;
	int _percent_create;
	int _percent_update;
	int _percent_delete;
	String _load_pattern;
	int _startSeqNum;
	String _prefix;
	String _suffix;
	String _template;
	int _expiry;
	int _size;
	int _start;
	int _end;
	String _dataFile;
	boolean _shuffle_docs;
	boolean isElasticSync;
	private String elasticIP;
	private String elasticPort;
	private String elasticLogin;
	private String elasticPassword;
	public static Faker faker = new Faker();

	public DocSpec(int _num_ops, int _percent_create, int _percent_update, int _percent_delete, String _load_pattern,
			int _startSeqNum, String _prefix, String _suffix, String _template, int _expiry, int _size, int _start,
			int _end, String _dataFile, boolean _shuffle_docs, boolean isElasticSync, String elasticIP, String elasticPort,
			String elasticLogin, String elasticPassword) {

		this._num_ops = _num_ops;
		this._percent_create = _percent_create;
		this._percent_update = _percent_update;
		this._percent_delete = _percent_delete;
		this._load_pattern = _load_pattern;
		this._startSeqNum = _startSeqNum;
		this._prefix = _prefix;
		this._suffix = _suffix;
		this._template = _template;
		this._expiry = _expiry;
		this._size = _size;
		this._start = _start;
		this._end = _end;
		this._dataFile = _dataFile;
		this._shuffle_docs = _shuffle_docs;
		this.isElasticSync = isElasticSync;
		this.setElasticIP(elasticIP);
		this.setElasticPort(elasticPort);
		this.setElasticLogin(elasticLogin);
		this.setElasticPassword(elasticPassword);
	}

	public int get_num_ops() {
		return _num_ops;
	}

	public int get_percent_create() {
		return _percent_create;
	}

	public int get_percent_update() {
		return _percent_update;
	}

	public int get_percent_delete() {
		return _percent_delete;
	}

	public String get_load_pattern() {
		return _load_pattern;
	}

	public int get_startSeqNum() {
		return _startSeqNum;
	}

	public String get_prefix() {
		return _prefix;
	}

	public String get_suffix() {
		return _suffix;
	}

	public String get_template() {
		return _template;
	}

	public long get_expiry() {
		return _expiry;
	}

	public int get_size() {
		return _size;
	}
	
	public int get_start() {
		return _start;
	}
	
	public int get_end() {
		return _end;
	}

	public String getDataFile() {
		return _dataFile;
	}
	
	public boolean get_shuffle_docs() {
		return _shuffle_docs;
	}

	public boolean isElasticSync() { return this.isElasticSync; }

	public void set_num_ops(int _num_ops) {
		this._num_ops = _num_ops;
	}

	public void set_percent_create(int _percent_create) {
		this._percent_create = _percent_create;
	}

	public void set_percent_update(int _percent_update) {
		this._percent_update = _percent_update;
	}

	public void set_percent_delete(int _percent_delete) {
		this._percent_delete = _percent_delete;
	}

	public void set_load_pattern(String _load_pattern) {
		this._load_pattern = _load_pattern;
	}

	public void set_startSeqNum(int _startSeqNum) {
		this._startSeqNum = _startSeqNum;
	}

	public void set_prefix(String _prefix) {
		this._prefix = _prefix;
	}

	public void set_suffix(String _suffix) {
		this._suffix = _suffix;
	}

	public void set_template(String _template) {
		this._template = _template;
	}

	public void set_expiry(int _expiry) {
		this._expiry = _expiry;
	}

	public void set_size(int _size) {
		this._size = _size;
	}
	
	public void set_start() {
		this._start = _start;
	}
	
	public void set_end() {
		this._end = _end;
	}
	
	public void setDataFile(String _dataFile) {
		this._dataFile = _dataFile;
	}
	
	public void set_shuffle_docs() {
		this._shuffle_docs = _shuffle_docs;
	}

	public void setElasticSync(boolean isElasticSync) { this.isElasticSync = isElasticSync; }

	public String getElasticIP() { return elasticIP; }

	public void setElasticIP(String elasticIP) { this.elasticIP = elasticIP; }

	public String getElasticPort() { return elasticPort; }

	public void setElasticPort(String elasticPort) { this.elasticPort = elasticPort; }

	public String getElasticLogin() { return elasticLogin; }

	public void setElasticLogin(String elasticLogin) { this.elasticLogin = elasticLogin; }

	public String getElasticPassword() { return elasticPassword; }

	public void setElasticPassword(String elasticPassword) { this.elasticPassword = elasticPassword; }
}
