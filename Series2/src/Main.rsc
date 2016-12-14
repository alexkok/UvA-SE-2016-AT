module Main

import DateTime;
import IO;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import Set;

// Our modules
import Config;
import TreeUtil;

//public loc prLoc = |project://MetricsTests2|;
public loc prLoc = |project://smallsql0.21_src|;
//public loc prLoc = |project://hsqldb-2.3.1|;
// Values to keep track of the current analysis
private loc projectLocation;
private bool useCache;
private bool isDebug;
private datetime startTime;
private datetime endTime;
private SubSequenceList subSequenceList;
private int maxSequenceLength;
private set[tuple[list[loc statement] statementLocations, loc fullLocation] clone] clones;

public void main(loc prLoc, bool cache = true, bool debug = true) {
	initializeDetector(prLoc, cache, debug);
	
	set[Declaration] asts = createAstsFromEclipseProject(prLoc, true);
	 //Cache.getAstsFromProject(projectLocation, useCache, isDebug);
	
	step1_createSequencesList(asts);
	step2_findCloneSequences();
	step3_showResults();
}

private void initializeDetector(loc prLoc, bool cache, bool debug) {
	startTime = now();
	projectLocation = prLoc;
	useCache = cache;
	isDebug = debug;
	println("**************** Clone Detector ****************");
	println("* Alex Kok                                     *");
	println("* Thanusijan Tharumarajah                      *");
	println("*                                              *");
	println("************************************************");
	println("* Start time:\t\t<printDateTime(startTime)>");
	println("* Project location:\t<projectLocation>");
	println("* Using cache:\t\t<useCache>");
	println("* Debug mode:\t\t<isDebug>");
	println("* Tresholds:");
	println("*  Min sequence length:\t<TRESHOLD_MIN_SEQUENCE_LENGTH>");
	println("************************************************");
}

private void step1_createSequencesList(set[Declaration] asts) {
	println("\>\>\> Phase 1: Creating the sequence list");
	int maximumNodes = getMaxNodesFromTrees(asts);
	int bucketSize = (maximumNodes > 9) ? maximumNodes / 10 : maximumNodes; // 10 % of the maximum nodes
	println("- Longest tree: <maximumNodes>");
	println("- Bucket size: <bucketSize>");
	subSequences = findSubSequences(asts, bucketSize, TRESHOLD_MIN_SEQUENCE_LENGTH);
	subSequenceList = subSequences.subSequenceList;
	maxSequenceLength = subSequences.maxSequenceLength;
	println("- Max sequence length: <maxSequenceLength>");
	println("- Current time: <printDateTime(now())>");
}

private void step2_findCloneSequences() {
	println("\>\>\> Phase 2: Finding clone sequences");
	clones = findDuplicateSequences(subSequenceList, maxSequenceLength, TRESHOLD_MIN_SEQUENCE_LENGTH);
	println("- Clones found: <size(clones)>");
	if (isDebug) {
		for (clone <- clones) {
			println("- Clone: <clone.fullLocation>");
		}
		println("- Clones found: <size(clones)>");
	}
}

private void step3_showResults() {
	endTime = now();
	println("******************* Finished *******************");
	println("* End time:\t\t <printDateTime(startTime)>");
	println("* Duration (y,m,d,h,m,s,ms): <createDuration(startTime, endTime)>");
	println("************************************************");
}