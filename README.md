# Pre-requisites

The project was built using the following stack:
1. Maven - 3.6.1
2. Java -  1.8.0_221
3. OS: MacOS Mojave (10.14.5)
4. IDE: Eclipse Version: 2019-06 (4.12.0)


# How to build?

1. Identify a folder in local disk where the project needs to be cloned. cd to that folder. Referring the folder as <INSTALL_ROOT>
2. execute: git clone https://github.com/pratyushtarar/myrepo.git
3. cd myrepo
4. mvn clean install

This will create executable jar file under the folder "target", created after the mvn command finishes. The name of executable jar file is "eval_exercise-0.0.1-SNAPSHOT-jar-with-dependencies.jar".

# How to run?

1. Start the mock server using the command:
	**java -cp "<INSTALLTION_FOLDER>/mockhttps-1.0-SNAPSHOT/lib/*" com.ebay.ads.https.HttpServer**
	where INSTALLTION_FOLDER = the folder where the tar provided is extracted.
2. Extract the input ".gz" files to a path on the local disk. The path upto the folder will be required for execution.
3. cd to <INSTALL_ROOT>/myrepo. Build to generate executable jar file. (instructions in "how to build" section).
4. Run the executable jar file using the following command:
5. Next step is to run the executable jar file. There are 6 arguments that the executable jar file takes. For detailed explanation of the  six arguments, either run the command: 
**java -jar target/eval_exercise-0.0.1-SNAPSHOT-jar-with-dependencies.jar** 

or 

refer to the main method documentation section of com.myrepo.exercise.UrlProcessingController.java

Following is a command that has been used to generate the output (can use those arguments for the run):
**java -jar <INSTALL_ROOT>/myrepo/target/eval_exercise-0.0.1-SNAPSHOT-jar-with-dependencies.jar 1000 20 15 6000 1000 <PATH_TO_FOLDER>**

where PATH_TO_FOLDER = the path of the folder containing GZ files. The program expects the gz file as input.

This will run and print the status in the console.

# How to interpret the console output (status)

There will be two reports that will be printed on the console:

1st report: 

This is the status of execution reported every 20 seconds. This realtime view of the progress (for logic refer to com.myrepo..exercise.StatusDaemon). In this report, the details will be:

name of file, status of processing (Refer com.myrepo.exercise.PorcessingStatus.java for list of statuses), % processing completed, succeeded requests (200 ok http response), failed requests (not 200 ok responses), and total records in the file

2nd report: 
This is the final execution report when all the files have been completely processed. The details of this report are:

Filename, % of total failed (not getting 200 ok response), % of total succeeded (having 200 ok reponse)
 


