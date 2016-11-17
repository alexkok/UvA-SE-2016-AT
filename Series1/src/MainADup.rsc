module MainADup

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

public loc prLoc = |project://MetricsTests2/src|;
//public loc prLoc = |project://smallsql0.21_src/src|;

public void main(loc project = prLoc) {
	println("Starting metrics analysis for duplication");
	
	M3 m = createM3FromEclipseProject(project);
	
	str theBigSrcFile = createBigFile(files(m));
	
	// Find duplication lines
	theList = findDuplications2(theBigSrcFile);
	iprintln(sort(theList));
	
	// Find the duplications of 6 lines or higher
	for (int key <- theList) {
		list[int] val = theList[key];
		
		for (v <- val) {
			if (key+6 in theList && v+6 in theList[key+6]) {
				if (key+5 in theList && v+5 in theList[key+5] && 
					key+4 in theList && v+4 in theList[key+4] && 
					key+3 in theList && v+3 in theList[key+3] && 
					key+2 in theList && v+2 in theList[key+2] && 
					key+1 in theList && v+1 in theList[key+1] ) {
					
					blockSize = 7;
					while (key+blockSize in theList && v+blockSize in theList[key+blockSize] ) {
						blockSize += 1;
					}
					println("Found dupl. block at <key> for <v> : size <blockSize>");
				}
			}
		}
	}
	//println(theBigSrcFile);
}

private str createBigFile(set[loc] files) {
// Remove tabs, remove comments
// if startswith /*, inComment = true;
// if contains */ inComment = false;
// ifstartswith // , removeline
// something like that
	str result = "";
	for (f <- files) {
		result += readFile(f);
		println("File: <f.path>");
		if (!endsWith(result, "\r\n")) {
			result += "\r\n";
		}
	}
	return result; 
}

private list[tuple[int, list[int], str]] findDuplications2(str src) {
	//list[str] lines = split("\r\n", src); // All lines
	list[str] lines = [trim(l) | l <- split("\r\n", src), !isEmpty(trim(l))]; // Lines without enters etc.
	int c1 = 0;
	int theSize = size(lines);
	println("Size: <theSize>");
	str checkingLine, tmpLine;
	
	list[tuple[int, list[int], str]] thelist = [];
	
	while (c1 < theSize) {
		checkingLine = lines[c1];
		println("Checking line [<c1>/<theSize>]");
		
		c2 = c1 + 1;
		while (c2 < theSize) {
			tmpLine = lines[c2];
			if (checkingLine == tmpLine) {
				//println("Found duplicate line <c1>|<c2> | <checkingLine>");
				if (c1 in domain(thelist)) {
					theList = for (<k, vs, strv> <- thelist) {
						if (k == c1) {
							vs += c2;
						}
					}
				} else {
					thelist += <c1, [c2], checkingLine>;
				}
			}
			//theMap[c1] = (c1 in theMap) ? theMap[c1] + c2 : [c2];
			
			c2 += 1;
		}
		
		
		c1 += 1;
	}
	
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