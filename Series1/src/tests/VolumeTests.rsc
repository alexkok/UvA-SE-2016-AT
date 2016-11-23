module tests::VolumeTests

import IO;
import String;
import List;

import MetricsUtil;

public test bool testDuplication_commentMultiLineAsOneLine() {
	str theSource = "package tests;
	'
	'public class DuplicationTests {
	'
	'	/* A multi comment that will act like a single line comment! */
	'	public void test() {
	'		
	'	}
	'
	'}";
	
	loc srcLocation = |project://MetricsTests2/gen/tests/testDuplication_commentMultiLineAsOneLine.java|;
	// Write the file so we can put it into the createBigFile method
	writeFile(srcLocation, theSource);
	
	tuple[str source, int extraLines] bigFile = createBigFile({srcLocation}, false);
	lines = split("\r\n", bigFile.source);
	result = size(lines) + bigFile.extraLines;
	//println(result);
	return result == 5;
}

public test bool testDuplication_commentMultiLine() {
	str theSource = "package tests;
	'
	'public class DuplicationTests {
	'
	'	/* A multi comment that will
	'	  spread onto multiple lines */
	'	public void test() {
	'		
	'	}
	'
	'}";
	
	loc srcLocation = |project://MetricsTests2/gen/tests/testDuplication_commentMultiLine.java|;
	// Write the file so we can put it into the createBigFile method
	writeFile(srcLocation, theSource);
	
	tuple[str source, int extraLines] bigFile = createBigFile({srcLocation}, false);
	lines = split("\r\n", bigFile.source);
	result = size(lines) + bigFile.extraLines;
	//println(result);
	return result == 5;
}

public test bool testDuplication_commentOneLine() {
	str theSource = "package tests;
	'
	'public class DuplicationTests {
	'
	'	// A single line comment!
	'	public void test() {
	'		
	'	}
	'
	'}";
	
	loc srcLocation = |project://MetricsTests2/gen/tests/testDuplication_commentOneLine.java|;
	// Write the file so we can put it into the createBigFile method
	writeFile(srcLocation, theSource);
	
	tuple[str source, int extraLines] bigFile = createBigFile({srcLocation}, false);
	lines = split("\r\n", bigFile.source);
	result = size(lines) + bigFile.extraLines;
	//println(result);
	return result == 5;
}