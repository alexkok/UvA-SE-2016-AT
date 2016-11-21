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
public loc prLoc = |project://smallsql0.21_src/src|;

public void main(loc project = prLoc) {
	println("Starting metrics analysis for duplication");
	
	M3 m = createM3FromEclipseProject(project);
	
	str theBigSrcFile = createBigFilev2(files(m));
	
	// Find duplication lines
	list[tuple[int lnNumber, list[int] dupLs, str dupStr]] theList = findDuplications2(theBigSrcFile);
	iprintln(sort(theList));
	
	// Find the duplications of 6 lines or higher
	println("Starting our logic");
	int curLnNumber = 0;
	int i = 0;
	while (i < size(theList)) {
		tup = theList[i];
		tupKey = indexOf(theList, tup);
		//println("Tuple: <tup> | <theList[tupKey]>");
		//list[int] val = theList[key];
		//
		if (tup.lnNumber >= curLnNumber) {
			//println("Ln:<tup.lnNumber>|i:<i>");
			int maxValue = 0;
			int j = 0;
			while (j < size(theList[tupKey].dupLs)) {
				v = theList[tupKey].dupLs[j];
				checkIndex = indexOf(domain(theList), tup.lnNumber+5);
				if (v+5 in theList[checkIndex].dupLs) {
					checkIndex4 = indexOf(domain(theList), tup.lnNumber+4);
					checkIndex3 = indexOf(domain(theList), tup.lnNumber+3);
					checkIndex2 = indexOf(domain(theList), tup.lnNumber+2);
					checkIndex1 = indexOf(domain(theList), tup.lnNumber+1);
					if (v+4 in theList[checkIndex4].dupLs &&
						v+3 in theList[checkIndex3].dupLs &&
						v+2 in theList[checkIndex2].dupLs &&
						v+1 in theList[checkIndex1].dupLs ) {
						
						str theDuplicate = tup.dupStr + theList[checkIndex1].dupStr + theList[checkIndex2].dupStr + theList[checkIndex3].dupStr + theList[checkIndex4].dupStr + theList[checkIndex].dupStr;
						blockSize = 6;
						while (v+blockSize in theList[indexOf(domain(theList), tup.lnNumber+blockSize)].dupLs) {
							theDuplicate += theList[indexOf(domain(theList), tup.lnNumber+blockSize)].dupStr;
 							blockSize += 1;
						}
						// The final check for real duplicates...
						if (v notin [tup.lnNumber..tup.lnNumber+blockSize-1]) {
							println("<tup.lnNumber>:<tup.lnNumber+blockSize-1>|<v>-<v+blockSize-1>| Found block! <tup.lnNumber> | <blockSize-1> | <substring(theDuplicate, 0, (size(theDuplicate) > 150) ? 150 : size(theDuplicate))>");
							j += blockSize-1;
							if (tup.lnNumber+blockSize-1 > maxValue) {
								maxValue = tup.lnNumber+blockSize-1;
							}
						}
					}
				}
				j += 1;
			}
			i+=1;
			if (maxValue != 0) {
				curLnNumber = maxValue;
			}
		} else {
			i+=1;
		}
	}
}

public str createBigFile(set[loc] files) {
// Remove tabs, remove comments
// if startswith /*, inComment = true;
// if contains */ inComment = false;
// ifstartswith // , removeline
// something like that
	str result = "";
	for (f <- files) {
		//str lines = readFile(f);
		result += readFile(f);
		//for (l <- split("\r\n", lines)) {
		//	//println("Checking line: <l>/<size(lines)>");
		//	if (!isEmpty(trim(l)) // Remove empty lines 
		//		&& !startsWith(trim(l), "//") // Remove single comment lines
		//		&& !startsWith(l, "package") // Remove package lines
		//		&& !startsWith(l, "import") // Remove import lines
		//		) {
		//		result += l + "\r\n";
		//	}
		//}
		
		if (!endsWith(result, "\r\n")) {
			result += "\r\n";
		}
	}
	return result; 
}

public str createBigFilev2(set[loc] files) {
	int i = 0;
	str result = "";
	bool inMultiComment = false;
	for (l <- [trim(l) | f <- files, l <- split("\r\n", readFile(f)), !isEmpty(trim(l)), !startsWith(trim(l), "//")]) { // Remove lines with only comments immediately
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
		println("[<i>]");
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
		//checkingLine = lines[c1];
		if (lastC1 != c1) {
			println("Checking line [<c1>/<theSize>]");
			lastC1 = c1;
		}
		
		//for (c2 <- [c1+1..theSize], checkingLine == lines[c2]) {
			//println("Found duplicate line <c1>|<c2> | <checkingLine>");
			if (c1 in domain(thelist)) {
				tupKey = indexOf(domain(thelist), c1);
				thelist[tupKey] = <c1, thelist[tupKey].dupLs + c2, lines[c1]>;
			} else {
				thelist += <c1, [c2], lines[c1]>;
			}
		//}
	}
	
	//for (c1 <- [0..theSize]) {
	//	checkingLine = lines[c1];
	//	println("Checking line [<c1>/<theSize>]");
	//	
	//	for (c2 <- [c1+1..theSize], checkingLine == lines[c2]) {
	//		//println("Found duplicate line <c1>|<c2> | <checkingLine>");
	//		if (c1 in domain(thelist)) {
	//			tupKey = indexOf(domain(thelist), c1);
	//			thelist[tupKey] = <c1, thelist[tupKey].dupLs + c2, checkingLine>;
	//		} else {
	//			thelist += <c1, [c2], checkingLine>;
	//		}
	//	}
	//}
	
	return thelist;
}

private void findDuplications(str src, int lineNrCount = 6) {
	//list[str] lines = split("\r\n", src); // All lines
	list[str] lines = [trim(l) | l <- split("\r\n", src), !isEmpty(trim(l))]; // Lines without enters etc.
	int c1 = 0;
	int theSize = size(lines);
	println("Size: <theSize>");
	str block1, block2;
	
	map[int, list[int]] theMap = ();
	
	while (c1 + lineNrCount < theSize) {
		block1 = createBlock(lines, theSize, c1, c1 + lineNrCount);
		//println("Checking block <c1>:<c1+lineNrCount>\t| <block1>");
		println("[<c1>/<theSize-lineNrCount>]");
		
		c2 = c1 + lineNrCount;
		while (c2 + lineNrCount < theSize) {
			print(".");
			block2 = createBlock(lines, theSize, c2, c2 + lineNrCount);
			//println("With block: \t\t\t| <block2>");
			if (block1 == block2) {
				print("\nFound duplicate <c1>:<c1+lineNrCount>|<c2>:<c2+lineNrCount> | <block1>");
				if (c1 in theMap) {
					theMap[c1] = theMap[c1] + c2;
					;
				} else {
					//theMap[c1] = [c2];
					theMap[c1] = [c2];
					;
				}
			}
			//theMap[c1] = (c1 in theMap) ? theMap[c1] + c2 : [c2];
			
			
			c2 += 1;
		}
		print("\n");
		
		
		c1 += 1;
	}
	
	iprintln(theMap);
	println("Map 133 : <theMap[133]>");
}


private str createBlock(lines, int theSize, c1, c2) {
	str result = "";
	for (i <- [0..theSize]) {
		// c1 <= i <= c2
		if (c1 <= i && i < c2) {
			result += lines[i];// + "\r\n";
		}
	}
	return result;
	
}