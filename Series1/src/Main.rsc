module Main

/**
 * Alex Kok
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah
 * thanus.tharumarajah@student.uva.nl
 */

import IO;
import DateTime;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import Prelude;
import ParseTree;
import vis::ParseTree;
import String;
import util::LOC;
import lang::java::m3::AST;
import lang::java::\syntax::Java15;

public void main() {
	loc projectLoc = |project://hsqldb-2.3.1/hsqldb/src/|;
	
	startTime = now();
	println("Starting metrics analysis \n<printDateTime(startTime)>");
	
	println("loc = <calculateVolume(projectLoc)>");
	
	endTime = now();
	println("<printDateTime(endTime)> \n<createDuration(startTime, endTime)>");
}

/**
 * Calculating the value of the volume.
 * 
 * By calculating the LOC of the programme and check these according to the ranking scheme that SIG uses.
 * Note that since we talk about Java projects, we can compare the values of man years based on their Java specifications. 
 * See the ranking scheme below.
 *
 ****************************************
 * // TODO: THE TABLE					*
 ****************************************
 * 
 * The metrics we use to evaluate the LOC
 * - Not comments or blank lines (SIG)
 * Some samples:
 * > /* ^/ + // aaa - Will be considered as a LOC >> (where ^ will be a *)
 * > When a { or } is found as their own on a line, it will still be considered as a LOC. 
 */ 
public int calculateVolume(loc project) {
	int sloc = 0;

	for(loc TS <- project) {
		try {
			if(isDirectory(TS)) {
				sloc += calculateVolume(TS);
			} else {
				Tree t = parse(#start[CompilationUnit], TS).top;
				sloc += countSLOC(t);			
			}
		} catch ParseError(loc l): {
			println("I found a parse error at line <l.begin.line>, column <l.begin.column>");
		}
	}
		
	return sloc;
}