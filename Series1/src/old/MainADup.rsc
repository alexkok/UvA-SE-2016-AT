module old::MainADup

/**
 * Alex Kok
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah
 * thanus.tharumarajah@student.uva.nl
 */

import IO;
import Prelude;
import String;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import lang::java::m3::AST;
import ParseTree;
import vis::ParseTree;
import util::LOC;
import lang::java::\syntax::Java15;
import Set;
import Map;

//public loc prLoc = |project://MetricsTests2/src|;
//public loc prLoc = |project://smallsql0.21_src/src|;
public loc prLoc = |project://hsqldb-2.3.1/hsqldb|;

public void main(loc project = prLoc) {
	println("Starting metrics analysis for duplication");
	
	// Print time
	analysisStartTime = now();
	println("- Start time:\t\t <printDateTime(analysisStartTime)>");
	
	M3 m = createM3FromEclipseProject(project);
	
	str theBigSrcFile = createBigFilev2(files(m));
	
	// Find duplication lines
	list[tuple[int lnNumber, list[int] dupLs, str dupStr]] theList = findDuplications2(theBigSrcFile);
	println("Got duplications list:");
	iprintln(theList);
	
	println("Starting our logic...");
	duplicates = calculateDuplicationBlocks(theList);
	println("Done, the duplicates:");
	for (str s <- duplicates) {
		println(s);
	}
	
	// Print time + duration
	println("\nDone");
	println("- Start time:\t\t <printDateTime(analysisStartTime)>");
	analysisEndTime = now();
	println("- End time: <printDateTime(analysisEndTime)>");
	println("- Analysis duration (y,m,d,h,m,s,ms): <createDuration(analysisStartTime, analysisEndTime)>");
}

public list[str] calculateDuplicationBlocks(list[tuple[int lnNumber, list[int] dupLs, str dupStr]] theList)  {
	list[str] duplicates = [];
	int curLnNumber = 0;
	for (i <- [0..size(theList)], theList[i].lnNumber >= curLnNumber) {
		tup = theList[i];
		println("[<i>/<size(theList)>]");

		int maxValue = 0;
		int j = 0;
		while (j < size(theList[i].dupLs)) {
			v = theList[i].dupLs[j];
			checkIndex = getIndexOf(theList, tup.lnNumber+5);
			if (v+5 in theList[checkIndex].dupLs) {
				checkIndex4 = getIndexOf(theList, tup.lnNumber+4);
				checkIndex3 = getIndexOf(theList, tup.lnNumber+3);
				checkIndex2 = getIndexOf(theList, tup.lnNumber+2);
				checkIndex1 = getIndexOf(theList, tup.lnNumber+1);
				if (v+4 in theList[checkIndex4].dupLs &&
					v+3 in theList[checkIndex3].dupLs &&
					v+2 in theList[checkIndex2].dupLs &&
					v+1 in theList[checkIndex1].dupLs ) {
					
					str theDuplicate = tup.dupStr + theList[checkIndex1].dupStr + theList[checkIndex2].dupStr + theList[checkIndex3].dupStr + theList[checkIndex4].dupStr + theList[checkIndex].dupStr;
					blockSize = 6;
					while (v+blockSize in theList[getIndexOf(theList, tup.lnNumber+blockSize)].dupLs) {
						theDuplicate += theList[getIndexOf(theList, tup.lnNumber+blockSize)].dupStr;
						blockSize += 1;
					}
					// The final check for real duplicates...
					if (v notin [tup.lnNumber..tup.lnNumber+blockSize-1]) {
						println("<tup.lnNumber>:<tup.lnNumber+blockSize-1>|<v>-<v+blockSize-1>| Found block! <tup.lnNumber> | <blockSize-1> | <substring(theDuplicate, 0, (size(theDuplicate) > 150) ? 150 : size(theDuplicate))>");
						duplicates += theDuplicate;
						j += blockSize-1;
						if (tup.lnNumber+blockSize-1 > maxValue) {
							maxValue = tup.lnNumber+blockSize-1;
						}
					}
				}
			}
			j += 1;
		}
		if (maxValue != 0) {
			curLnNumber = maxValue;
		}
	}
	return duplicates;
}

public str createBigFilev2(set[loc] files) {
	int i = 0;
	str result = "";
	bool inMultiComment = false;
	for (l <- [trim(l) | f <- files, 
						 l <- split("\r\n", readFile(f)), 
						 !isEmpty(trim(l)), 
						 !startsWith(trim(l), "//"),  // Remove lines with only comments immediately 
						 trim(l) != "}", // Remove lines that only consist of a "}" 
						 !startsWith(trim(l), "package"), // Remove package lines 
						 !startsWith(trim(l), "import") // Remove import lines
						 ]) {
		// Remove comment lines immediately (containing "//" and not containing "*//*")
		if (contains(l, "//") && !contains(l, "*//*")) {
			l = split("//", l)[0];
		}
		if (!inMultiComment && contains(l, "/*")) {
			// Opening a multi line comment
			inMultiComment = true;
			if (!startsWith(trim(l), "/*")) {
				// Multi line comment is started on a LOC
				result += split("/*", l)[0] + "\r\n";
			}
		} else if (inMultiComment && contains(l, "*/")) {
			inMultiComment = false;
			if (size(split(l, "\\*/")) > 1 && contains(split("\\*/")[1], "/*")) {
				// Closing, but also starting another multi comment
				commentOpen = true;
				if (startsWith(trim(split("\\*/", l)[1]), "/*")) {
					// Not starting with /*, so it has a LOC. Add it.
					result += l  + "\r\n";
				}
			}
		} else if (!inMultiComment) {
			result += l + "\r\n";
		} // Else: Comment is open, ignore this line
		println("[<i>] <l>");
		i += 1;
	}
	return result;
}

public list[tuple[int, list[int], str]] findDuplications2(str src) {
	list[str] lines = [trim(l) | l <- split("\r\n", src)]; 
	int theSize = size(lines);
	println("Size: <theSize>");
	str checkingLine;
	
	list[tuple[int lnNumber, list[int] dupLs, str dupStr]] thelist = [];
	
	int lastC1 = 0;
	for (<c1,c2> <- [<c1,c2> | c1 <- [0..theSize], c2 <- [c1+1..theSize], lines[c1] == lines[c2]]) {
		if (lastC1 != c1) {
			println("Checking line [<c1>/<theSize>]");
			lastC1 = c1;
		}
		
		tupKey = getIndexOf(thelist, c1);
		if (tupKey != -1) { 
			// If found, merge this value in the list
			thelist[tupKey] = <c1, thelist[tupKey].dupLs + c2, lines[c1]>;
		} else {
			// -1 means "not found", add new value to the list
			thelist += <c1, [c2], lines[c1]>;
		}
	}
	
	return thelist;
}

private int getIndexOf(list[tuple[int lnNumber, list[int] dupLs, str dupStr]] tupleList, int lineNumber) {
	for (int i <- [0..size(tupleList)]) {
		if (tupleList[i].lnNumber == lineNumber) return i;
	}
	return -1;
}