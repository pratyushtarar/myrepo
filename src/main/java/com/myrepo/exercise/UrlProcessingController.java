/*
 * 
 */
package com.myrepo.exercise;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * Processor class. 
 * 
 * High level approach is to process different files in different threads to optimize resources. 
 * The number of files that get processed in parallel will be capped to a max. 
 * Also, every file is having thousands of URLs. Thus, to improve parallelism, each file will
 * also get processed by multiple threads for optimization. Again, there will be a cap on the number of threads
 * that processes a single file.
 * Finally, there is a large number of IO operations that happen in the current data set. The
 * optimization in IO is achieved by closing expired sessions and also putting a cap on the number of max connections
 * that can be made during the processing of input data. This is achieved by using PoolingHttpClientConnectionManager
 * offered by apache http client libraries. 
 * 
 *  java -jar target/eval_exercise-0.0.1-SNAPSHOT-jar-with-dependencies.jar 1000 20 5 60000 10000 /Users/ptarar/Downloads/URLFileProcessorAssignment/inputData
 *
 * @author ptarar
 *
 */

public class UrlProcessingController {

	/** The cache for application. */
	Cache cache = Cache.getInstance();
	
	/**
	 * The process method. THe method plays the role of orchestrating the spawning of threads.
	 *
	 * @param tuningParams : map of tuning params
	 * @param inputData : path of the folder containing the gzip files		
	 */
	public void process(Map<String, Integer> tuningParams, String inputData) {
		long startTime = Calendar.getInstance().getTimeInMillis();
		
		Set<Path> files = new HashSet<Path>();
		Cache cache = Cache.getInstance();
		try {
			files = getFiles(inputData);
			ExecutorService executor = Executors.newFixedThreadPool(tuningParams.get("CONCURRENT_FILES_PORCESSING_COUNT"));
			PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
			connManager.setMaxTotal(tuningParams.get("MAX_HTTP_CONNECTIONS_COUNT"));
			connManager.setDefaultMaxPerRoute(tuningParams.get("MAX_HTTP_DEFAULT_PER_ROUTE"));
			for (Path path : files) {
				long linesCount = getFileLinesCount(path);
				String[] pathSegment = path.toString().split("/");
				cache.setFileSizeCache(pathSegment[pathSegment.length - 1], linesCount);
				for (int counter = 0; counter < files.size();) {
					Runnable runnable = new FileProcessor(path, linesCount, tuningParams.get("LIMIT"), connManager, tuningParams.get("THREADS_PER_FILE_COUNT"));
					executor.execute(runnable);
					counter = counter + tuningParams.get("LIMIT");
				}
			}
			executor.shutdown();
			ExecutorService statusDaemon = Executors.newSingleThreadExecutor();
			Runnable r = new StatusDaemon(startTime);
			statusDaemon.execute(r);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * The entry point of the application. Expects 6 arguments. The arguments are:
	 * 0 - LIMIT: As per the design, every single file will be processed by multiple threads. 'LIMIT' defines the number of rows in a file that should be processed by a single thread.
	 * 1 - CONCURRENT_FILES_PORCESSING_COUNT: As per the design, multiple files will be processed simultaneously. 'CONCURRENT_FILES_PORCESSING_COUNT' defines the number of files that gets processed in parallel.
	 * 2 - THREADS_PER_FILE_COUNT: As per the design, every single file will be processed by multiple threads. 'THREADS_PER_FILE_COUNT' defines the MAX count of threads that should process any single file. Thus, the total number of threads that will be run = CONCURRENT_FILES_PORCESSING_COUNT X THREADS_PER_FILE_COUNT. 
	 * 	   For this exercise, CONCURRENT_FILES_PORCESSING_COUNT has been set of the number of files in input data.
	 * 3 - MAX_HTTP_CONNECTIONS_COUNT: Maximum number of the http connections that are configured in http client connection pool. used for performance tuning of http client.
	 * 4 - MAX_HTTP_DEFAULT_PER_ROUTE: Maximum number of the http connections that are configured in http client connection pool per route.
	 * 5 - inputData: The path of the folder that contains "gz" file containing the input data.
	 * 
	 * @param args the arguments supplied by the user
	 * 
	 */
	public static void main(String[] args) throws Exception {
		if (args!=null && args.length != 6) {
			System.out.println("All paramters not provided. Following expected: \n"
					 + " 1st arg - LIMIT: As per the design, every single file will be processed by multiple threads. 'LIMIT' defines the number of rows in a file that should be processed by a single thread. \n"
					 + " 2nd arg - CONCURRENT_FILES_PORCESSING_COUNT: As per the design, multiple files will be processed simultaneously. 'CONCURRENT_FILES_PORCESSING_COUNT' defines the number of files that gets processed in parallel. \n"
					 + " 3rd arg - THREADS_PER_FILE_COUNT: As per the design, every single file will be processed by multiple threads. 'THREADS_PER_FILE_COUNT' defines the MAX count of threads that should process any single file. \n"
					 + " 4th arg - MAX_HTTP_CONNECTIONS_COUNT: Maximum number of the http connections that are configured in http client connection pool. used for performance tuning of http client. \n"
					 + " 5th arg - MAX_HTTP_DEFAULT_PER_ROUTE: Maximum number of the http connections that are configured in http client connection pool per route. \n"
					 + " 6th arg - inputData: The path of the folder that contains 'gz' file containing the input data."
					);
			System.exit(0);
		}
		Map<String, Integer> tuningParams = new HashMap<String, Integer>();
		String inputData = null;
		try {
			inputData = new String(args[5].getBytes(), "UTF-8");
			tuningParams.put("LIMIT", new Integer(args[0]));
			tuningParams.put("CONCURRENT_FILES_PORCESSING_COUNT", new Integer(args[1]));
			tuningParams.put("THREADS_PER_FILE_COUNT", new Integer(args[2]));
			tuningParams.put("MAX_HTTP_CONNECTIONS_COUNT", new Integer(args[3]));
			tuningParams.put("MAX_HTTP_DEFAULT_PER_ROUTE", new Integer(args[4]));
		} catch (UnsupportedEncodingException e) {
			System.out.println("Input argument format not correct");
			throw e;
		}
		UrlProcessingController processor = new UrlProcessingController();
		//String[] args1 = {"5","1","1","1","1","/Users/pratyusht/Downloads/URLFileProcessorAssignment/test"};
		processor.process(tuningParams, inputData);
	}


	/**
	 * Gets the set of files. Additionally the method populates the cache with "unprocessed" status.
	 *
	 * @param inputData the input data folder path
	 * @return the set of files
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Set<Path> getFiles(String inputData) throws IOException {
		Set<Path> filesSet = new HashSet<Path>();
		Files.list(Paths.get(inputData.trim()))
				.filter(Files::isRegularFile).forEach((x) -> {
					try {
						// (int) Files.size(x)) will give the size of each file. 
						// As further optimization, the size can be captured for each file and used to optimize the number of threads that process that
						// that file. For the scope of this solution, same number of threads will be allocated to each file irrespective of their size.
						filesSet.add(x);
						String[] pathSegment = x.toString().split("/");
						cache.setProcessingStatus(pathSegment[pathSegment.length - 1], ProcessingStatus.UNPROCESSED);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
		return filesSet;
	}

	/**
	 * Gets the file lines count.
	 *
	 * @param path the path
	 * @return the file lines count
	 */
	public long getFileLinesCount(Path path) {
		long linesCount = -1L;
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))))) {
			linesCount = reader.lines().count();
			reader.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return linesCount;
	}

}
