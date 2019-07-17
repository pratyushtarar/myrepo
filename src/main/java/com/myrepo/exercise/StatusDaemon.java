package com.myrepo.exercise;

import java.text.DecimalFormat;
import java.util.Calendar;

/**
 * The Class StatusDaemon which regularly prints out the status of the execution which includes progress %. 
 * Once the thread execution completes, it gives a final summary.
 * 
 * @author ptarar
 */
public class StatusDaemon implements Runnable {

	/** The start time. */
	private long startTime;

	/**
	 * Instantiates a new status daemon.
	 *
	 * @param startTime the start time
	 */
	public StatusDaemon(long startTime) {
		this.startTime = startTime;
	}

	/**
	 * Run.
	 */
	public void run() {
		while (true) {
			long timeElapsedInSec = Calendar.getInstance().getTimeInMillis() - startTime;
			System.out.println("				  Time elapsed = " + timeElapsedInSec / 60000 + " minutes ("
					+ timeElapsedInSec / 1000 + " seconds)");
			if (Cache.getInstance() != null && Cache.getInstance().getFileSizeCache() != null) {
				for (String filename : Cache.getInstance().getFileSizeCache().keySet()) {
					if (Cache.getInstance().getFailedRequestsCache().get(filename) != null
							&& Cache.getInstance().getSucceededRequestsCache().get(filename) != null
							&& Cache.getInstance().getFileSizeCache().get(filename) != null) {
						Cache.getInstance().printProcessingStatus();
					}
				}
				if (Cache.getInstance().isDoneStatus()) {
					break;
				}
			}

			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("ALL files processed...");
		System.out.println("Summary of Processing:");
		if (Cache.getInstance() != null && Cache.getInstance().getFileSizeCache() != null) {
			DecimalFormat df = new DecimalFormat("0.00");
			for (String filename : Cache.getInstance().getFileSizeCache().keySet()) {
				if (Cache.getInstance().getFailedRequestsCache().get(filename) != null
						&& Cache.getInstance().getSucceededRequestsCache().get(filename) != null
						&& Cache.getInstance().getFileSizeCache().get(filename) != null) {

					double failedPer = (double) Cache.getInstance().getFailedRequestsCache().get(filename)
							/ (Cache.getInstance().getFileSizeCache().get(filename));
					double successPer = (double) Cache.getInstance().getSucceededRequestsCache().get(filename)
							/ (Cache.getInstance().getFileSizeCache().get(filename));
					System.out.println("Filename = " + filename + " failed = " + df.format(100 * failedPer) + "%"
							+ ", succeeded = " + df.format(100 * successPer) + "%");
				}

			}
		}

	}

}
