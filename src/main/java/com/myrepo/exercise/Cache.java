/*
 * 
 */
package com.myrepo.exercise;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Singleton class which acts as in-memory cache. All threads in the application will get access to one single 
 * instance of this Cache class. To make it update of cache thread-safe, ConcurrentHashMap is used where ever applicable.
 */
public class Cache {
	
	/** The instance. */
	private static Cache instance = null;
	
	/** The cache. */
	private ConcurrentHashMap<String, ProcessingStatus> cache = null;
	
	/** The failed requests cache. */
	private ConcurrentHashMap<String, Long> failedRequestsCache = null;
	
	/** The succeeded requests cache. */
	private ConcurrentHashMap<String, Long> succeededRequestsCache = null;
	
	/** The file size cache. */
	private Map<String, Long> fileSizeCache = null;
	
	/** The lock object. */
	private static Object lock = new Object();

	/**
	 * Instantiates a new cache.
	 */
	private Cache() {
		super();
		cache = new ConcurrentHashMap<String, ProcessingStatus>();
		failedRequestsCache = new ConcurrentHashMap<String, Long>();
		succeededRequestsCache = new ConcurrentHashMap<String, Long>();
		fileSizeCache = new HashMap<String, Long>();
	}
	
	/**
	 * Gets the single instance of Cache.
	 *
	 * @return single instance of Cache
	 */
	public static Cache getInstance() {
		synchronized(lock) {
			if(instance == null )
				instance = new Cache();			
		}
		
		return instance;
	}
	
	/**
	 * Gets the processing status.
	 *
	 * @param fileName the file name
	 * @return the processing status
	 */
	public ProcessingStatus getProcessingStatus(String fileName) {
		return cache.get(fileName);
	}
	
	/**
	 * Gets the file size cache.
	 *
	 * @return the file size cache
	 */
	public Map<String, Long> getFileSizeCache() {
		return fileSizeCache;
	}
	
	/**
	 * Gets the failed requests cache.
	 *
	 * @return the failed requests cache
	 */
	public ConcurrentHashMap<String, Long> getFailedRequestsCache() {
		return failedRequestsCache;
	}
	
	/**
	 * Gets the succeeded requests cache.
	 *
	 * @return the succeeded requests cache
	 */
	public ConcurrentHashMap<String, Long> getSucceededRequestsCache() {
		return succeededRequestsCache;
	}
	
	/**
	 * Set processing status.
	 *
	 * @param fileName the file name
	 * @param status the status
	 */
	public void setProcessingStatus(String fileName, ProcessingStatus status) {
		 cache.put(fileName, status);
	}
	
	/**
	 * Sets the file size cache.
	 *
	 * @param fileName the file name
	 * @param size the size
	 */
	public void setFileSizeCache(String fileName, Long size) {
		fileSizeCache.put(fileName, size);
	}
	
	/**
	 * Increment failed count.
	 *
	 * @param fileName the file name
	 */
	public synchronized void incrementFailedCount(String fileName) {
		long counter = 1;
		 if(failedRequestsCache.get(fileName) != null) {
			 counter = 1+failedRequestsCache.get(fileName).longValue(); 
		 }
		 failedRequestsCache.put(fileName, counter);
	}
	
	/**
	 * Increment succeeded count.
	 *
	 * @param fileName the file name
	 */
	public synchronized void incrementSucceededCount(String fileName) {
		long counter = 1;
		 if(succeededRequestsCache.get(fileName) != null) {
			 counter = 1+succeededRequestsCache.get(fileName).longValue(); 
		 }
		 succeededRequestsCache.put(fileName, counter);
	}
	
	/**
	 * Prints the processing status.
	 */
	public void printProcessingStatus() {
		System.out.println("####################################################################################################################################");
		DecimalFormat df = new DecimalFormat("0.00");
		for(String filename : cache.keySet()) {
			if(cache.get(filename) != null && fileSizeCache.get(filename)  != null && failedRequestsCache.get(filename) != null && succeededRequestsCache.get(filename) != null) {
			System.out.println("Filename = " + filename + " status = " + cache.get(filename).name() 
					+ " Processing Completed = " + df.format((double) 100*(failedRequestsCache.get(filename)+succeededRequestsCache.get(filename))/fileSizeCache.get(filename))
					+ "%, where succeeded = " +  succeededRequestsCache.get(filename) 
					+ ", failed = " +  failedRequestsCache.get(filename)
					+ " and total = " + fileSizeCache.get(filename));
			}
		}
	}
	
	/**
	 * Checks if is done status.
	 *
	 * @return true, if is done status
	 */
	public boolean isDoneStatus() {
		for(String filename : cache.keySet()) {
			if(!cache.get(filename).equals(ProcessingStatus.DONE))
				return false;
		}
		return true;
	}
}
	
