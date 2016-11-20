module oldmetrics::UnitSize

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
import lang::java::m3::Core;
import lang::java::m3::AST;
import lang::java::jdt::m3::AST;
import lang::java::jdt::m3::Core;
import lang::java::\syntax::Java15;

public void main() {
	startTime = now();
	println("Starting unit size analysis \n<printDateTime(startTime)>");
	
	loc project = |project://MetricsTests2/src/main/|;
	calculateUnitSize(project);
	
	endTime = now();
	println("<printDateTime(endTime)> \n<createDuration(startTime, endTime)>");
}

public list[tuple[loc, int]] calculateUnitSize(loc project) {
	myModel = createM3FromEclipseProject(project);
	myMethods = methods(myModel);
	
	return [<method, calculateVolumeOfUnit(method)> | method <- myMethods];
}

public int calculateVolumeOfUnit(loc project) {
	int sloc = 0;
	
	try {
		methodSrc = readFile(project);
		Tree t = parse(#MethodDec, methodSrc);
		sloc += countSLOC(t);				
	} catch ParseError(loc l): {
		println("I found a parse error at line <l.begin.line>, column <l.begin.column>");
	}
	
	return sloc;
}