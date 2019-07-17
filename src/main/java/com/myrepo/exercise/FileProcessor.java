/*
 * 
 */
package com.myrepo.exercise;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * The Class FileProcessor. Each gz file will have dedicated thread implemented in this FileProcessor class.
 * The FileProcessor will in spawn parallel threads up to the LIMIT to process the file.
 */
public class FileProcessor implements Runnable {
	
	/** The path. */
	private Path path;
	
	/** The limit. */
	private int limit;
	
	/** The lines count. */
	private long linesCount;
	
	/** The conn manager. */
	private PoolingHttpClientConnectionManager connManager;
	
	/** The threads per file count. */
	private int threadsPerFileCount;
	
	/**
	 * Instantiates a new file processor.
	 *
	 * @param path the path
	 * @param linesCount the lines count
	 * @param limit the limit
	 * @param connManager the conn manager
	 * @param threadsPerFileCount the threads per file count
	 */
	public FileProcessor(Path path, long linesCount, int limit, PoolingHttpClientConnectionManager connManager, int threadsPerFileCount) {
		this.path = path;
		this.limit = limit;
		this.linesCount = linesCount;
		this.connManager = connManager;
		this.threadsPerFileCount = threadsPerFileCount;
	}
	
	/**
	 * Run.
	 */
	public void run() {
		ExecutorService executor1 = Executors.newFixedThreadPool(threadsPerFileCount);
		for(long counter = 0; counter < linesCount;) {
			Runnable runnable = new UrlProcessor(path, counter, limit, linesCount, connManager);
			executor1.execute(runnable);
			String[] pathSegment = path.toString().split("/");
			Cache.getInstance().setProcessingStatus(pathSegment[pathSegment.length-1], ProcessingStatus.PROCESSING);
			counter = counter + limit;
		}
		executor1.shutdown();
	}
}
