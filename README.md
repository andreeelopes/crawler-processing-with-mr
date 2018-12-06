#Project structure

	.
	├───crawler-processing-with-MR
	│   ├───src
	│   │   └───main
	│   │       ├───java
	│   │       │   ├───mapreducers		// job logic
	│   │       │   └───utils
	│   │       └───scripts				// setup and test scripts
	│   └───target						// jar location
	├───doc								// report
	└───README.md

#Generate executable

In the crawler-processing-with-MR:
`mvn package` 
This will generate a jar in the target folder

#Deploy
1. Upload the jar to the /tests folder in the cluster
2. Upload setupTestEnv.sh to the the cluster and run it
3. Upload testQuery.sh to the the cluster and run it. This will run the tests for the Network Performance job.
4. Collect results present in /tests/mr-results
