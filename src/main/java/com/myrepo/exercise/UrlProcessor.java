package com.myrepo.exercise;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;

/**
 * The Class UrlProcessor. The thread that processes a set of lines of a specific file. What lines are processed by this thread 
 * are provided as the input to this thread.
 * 
 * @author ptarar
 */
public class UrlProcessor implements Runnable {

	/** The path. */
	private Path path;
	
	/** The start line. */
	private long startLine;
	
	/** The limit. */
	//private long endLine;
	private int limit;
	
	/** The lines count. */
	private long linesCount;
	
	/** The conn manager. */
	private PoolingHttpClientConnectionManager connManager;

	/**
	 * Instantiates a new url processor.
	 *
	 * @param path the path
	 * @param startLine the start line
	 * @param limit the limit
	 * @param linesCount the lines count
	 * @param connManager the conn manager
	 */
	public UrlProcessor(Path path, long startLine, int limit, long linesCount,
			PoolingHttpClientConnectionManager connManager) {
		this.path = path;
		this.startLine = startLine;
		this.limit = limit;
		this.linesCount = linesCount;
		this.connManager = connManager;
	}

	/**
	 * Run method of thread.
	 */
	public void run() {
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))));
			long skipCounter = 0;
			// This logic results in reading the entire file by all the threads there by increasing the IO. 
			// Alternatively, RandomAccessFile approach can be used to optimize. for the scope of this exercise, 
			// the design uses skipping file logic.
			while (skipCounter < startLine) {
				String str = bufferedReader.readLine();
				skipCounter++;
			}
			long counter = 0;
			while (counter < limit) {
				String url = bufferedReader.readLine();
				String[] pathSegment = path.toString().split("/");
				if (null != url && url.length() > 7) {
					CloseableHttpClient client = HttpClients.custom().setConnectionManager(connManager).build();
					HttpGet request = new HttpGet(url);
					request.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
					HttpResponse response = client.execute(request);
					if(response.getStatusLine().getStatusCode() == 200) {
						Cache.getInstance().incrementSucceededCount(pathSegment[pathSegment.length - 1]);
					} else {
						Cache.getInstance().incrementFailedCount(pathSegment[pathSegment.length - 1]);
					}
					connManager.closeExpiredConnections();
				} else {
					Cache.getInstance().incrementFailedCount(pathSegment[pathSegment.length - 1]);
				}				
				if (counter + startLine == (linesCount-1)) {
					Cache.getInstance().setProcessingStatus(pathSegment[pathSegment.length - 1],
							ProcessingStatus.DONE);
					Cache.getInstance().printProcessingStatus();
				}
				if ((counter + startLine) > linesCount-1) {
					break;
				}
				counter++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
