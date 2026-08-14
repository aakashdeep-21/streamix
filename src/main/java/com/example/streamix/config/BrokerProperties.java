package com.example.streamix.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

// All broker tuning knobs; every field is overridable via env vars (12-factor).
@ConfigurationProperties(prefix = "streamix")
public class BrokerProperties {

	private String storage = "file";
	private String dataDir = "./data";
	private long defaultSessionTimeoutMs = 30_000;
	private long minSessionTimeoutMs = 1_000;
	private long maxSessionTimeoutMs = 3_600_000;
	private long sessionSweepMs = 1_000;
	private int pollDefaultMax = 100;
	private int pollMaxLimit = 1_000;
	private int batchMaxSize = 500;
	private int maxPartitionsPerTopic = 64;
	private int maxMessageBytes = 1_048_576;

	public String getStorage() { return storage; }
	public void setStorage(String storage) { this.storage = storage; }
	public String getDataDir() { return dataDir; }
	public void setDataDir(String dataDir) { this.dataDir = dataDir; }
	public long getDefaultSessionTimeoutMs() { return defaultSessionTimeoutMs; }
	public void setDefaultSessionTimeoutMs(long v) { this.defaultSessionTimeoutMs = v; }
	public long getMinSessionTimeoutMs() { return minSessionTimeoutMs; }
	public void setMinSessionTimeoutMs(long v) { this.minSessionTimeoutMs = v; }
	public long getMaxSessionTimeoutMs() { return maxSessionTimeoutMs; }
	public void setMaxSessionTimeoutMs(long v) { this.maxSessionTimeoutMs = v; }
	public long getSessionSweepMs() { return sessionSweepMs; }
	public void setSessionSweepMs(long v) { this.sessionSweepMs = v; }
	public int getPollDefaultMax() { return pollDefaultMax; }
	public void setPollDefaultMax(int v) { this.pollDefaultMax = v; }
	public int getPollMaxLimit() { return pollMaxLimit; }
	public void setPollMaxLimit(int v) { this.pollMaxLimit = v; }
	public int getBatchMaxSize() { return batchMaxSize; }
	public void setBatchMaxSize(int v) { this.batchMaxSize = v; }
	public int getMaxPartitionsPerTopic() { return maxPartitionsPerTopic; }
	public void setMaxPartitionsPerTopic(int v) { this.maxPartitionsPerTopic = v; }
	public int getMaxMessageBytes() { return maxMessageBytes; }
	public void setMaxMessageBytes(int v) { this.maxMessageBytes = v; }
}
