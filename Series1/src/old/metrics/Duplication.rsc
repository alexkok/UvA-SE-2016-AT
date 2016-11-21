module oldmetrics::Duplication

/**
 * Alex Kok
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah
 * thanus.tharumarajah@student.uva.nl
 */

import Prelude;
import ParseTree;
import util::LOC;
import util::FileSystem;
import lang::java::m3::Core;
import lang::java::m3::AST;
import lang::java::jdt::m3::AST;
import lang::java::jdt::m3::Core;
import lang::java::\syntax::Java15;
import vis::Figure;
import vis::ParseTree;
import vis::Render;

public void main() {
	startTime = now();
	println("Starting unit size analysis \n<printDateTime(startTime)>");
	
	loc project = |project://MetricsTests2/src/main/|;
	//calculateDuplication(project);
	
	endTime = now();
	println("<printDateTime(endTime)> \n<createDuration(startTime, endTime)>");
}

public str total(loc project) {
	M3 myModel = createM3FromEclipseProject(project);
	str lines = "";
	
	for(file <- files(myModel)) {
		//lines += readFileLines(file);
		lines = lines + ("" | it + fileLines | str fileLines <- readFileLines(file));
	}
	
	return lines;
}

public void findDuplicates() {
	loc project = |project://MetricsTests2/src/main/|;
	M3 myModel = createM3FromEclipseProject(project);
	
	totalLines = total(project);
	
	map[str,int] m = ();
	rel[int origin,int duplicate] dup_list={};
	
	for(file <- files(myModel)) {
		list[str] fileLines = readFileLines(file);
		println(file);
		
		for(line <- [0..size(fileLines)]) {
			//checkDuplicateTillEnd(line, size(fileLines), fileLines, m, dup_list);
			int lineNumber = line;
			int lineNumberTill = line + 5;
	
			while(lineNumberTill < size(fileLines)) {
				list[str] block = fileLines[lineNumber..lineNumberTill];
	
				//println("<lineNumber> : <lineNumberTill>");
				println(block);
				
				strBlock = ("" | it + a | str a <- block);
				//println(strBlock);
				if (contains(totalLines, strBlock)) {
					println("+");
					//println(lineNumber);
					//dup_list = dup_list + <m[strBlock],lineNumber>;
					// is duplicate
				} else {
					//m = m+ (strBlock:lineNumber);
					println("-");
					// niet duplicate
				}
			
				lineNumberTill += 1;
			}
			
			//break;
		}
		
		//break;
	}
	
	println(dup_list);
}

//public void checkDuplicateTillEnd(int lineNumber, int maxSize, list[str] fileLines, map[str,int] m, rel[int origin,int duplicate] dup_list) {
//	int lineNumberTill = lineNumber + 1;
//	
//	while(lineNumberTill < maxSize) {
//		list[str] block = fileLines[lineNumber..lineNumberTill];
//	
//		println("<lineNumber> : <lineNumberTill>");
//		println(block);
//		println();
//		
//		//if(block in m) {
//		//	println("a");
//  //    		dup_list = dup_list + <m[block],lineNumber>;
//  //    	} else {
//		//	m = m+ (block:lineNumber);
//		//}
//		
//		lineNumberTill += 1;
//	}
//	
//}


//subset = ("" | it + a | str a <- fileLines);
//allLines = ("" | it + a | str a <- totalLines);

//str myFunction(map[str,str] params) = "Hello World!";
//serve(|http://localhost:8081|, functionserver(("/index.html": myFunction)))