module MainA

/**
 * Alex Kok
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah
 * thanus.tharumarajah@student.uva.nl
 */

import IO;
//import List;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import lang::java::m3::AST;
import ParseTree;
import util::LOC;
import lang::java::\syntax::Java15;

public loc prLoc = |project://MetricsTests2/src|;
//public M3 m = createM3FromEclipseProject(prLoc);
//public M3 m = createM3FromEclipseProject(|project://MetricsTests2|);
//public Declaration d = getMethodASTEclipse(|java+method:///main/Main/main(java.lang.String%5B%5D)|, model=m);

public void main2() {
	println("Starting metrics analysis");
	
	//m = createM3FromEclipseProject(|project://MetricsTests2|);
	//m = createM3FromEclipseProject(|project://smallsql0.21_src|);
	names = m@names;
	//iprint(methods(m));
	//iprint(m@containment);
	//iprint(m@containment[|java+class:///main/Main|]);
	//[ e | e <- m@containment[|java+class:///main/Main|], e.scheme == "java+method"];
	
	cl = |java+class:///main/Main|;
	x = size([ st | st <- m@containment[cl], isMethod(st)]);
	println(x);
	
	//map[loc class, int methodCount] numberOfMethodsPerClass = (cl:numberOfMethods(cl, m) | <cl,_> <- m@containment, isClass(cl));
	//> map[loc class, int methodCount]: (
	//   |java+class:///Fruit|:1,
	//   |java+class:///Apple|:1,
	//   |java+class:///HelloWorld|:1
	// )
	//theLocation = getOneFrom(files(m));
	theLocation = prLoc + "main/Main.java";
	//theLocation = |project://MetricsTests2/src/main/Main.java|;
	println("The location: <theLocation>");
	try {
		t = parse(#CompilationUnit, theLocation);
		println(countSLOC(t));
	} catch ParseError(loc l): {
		println("I found a parse error at line <l.begin.line>, column <l.begin.column>");
	}
}

public int calculateLOC(loc location) {
	try {
		//t = parse(#start[CompilationUnit], location);
		t = parse(#MethodDec, location);
		countSLOC(t);
		return countSLOC(t);
	} catch ParseError(loc l): {
		println("<readFile(l)>");
		println("Found a parse error at line <l.begin.line>, column <l.begin.column>");
		return -1;
	}
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
public list[tuple[loc, int]] filesLOC(loc root) {
	list[tuple[loc, int]] result = [<ls, calculateLOC(ls)> | ls <- root.ls, !isDirectory(ls)];
	for (nl <- [rst | rst <- root.ls, isDirectory(rst)]) { // The dirs
		result += filesLOC(nl);
	}
	return result;
}

public int calculateVolume(loc project) {
	iprintln(filesLOC(project));
	return (0 | it + e | <_, e> <- filesLOC(project), e != -1); // -1 means it was an error.
}

public void calculateUnitSize(loc project) {
	println("Creating M3 model from project");
	M3 m = createM3FromEclipseProject(project);
	
	println("Calculating LOC per method");
	list[tuple[loc, int]] methodsLOC = [<l,calculateLOC(l)> | l <- methods(m)];
	iprintln(methodsLOC);
	
	map[int, int] amountPerCategory = (1: 0, 2: 0, 3: 0, 4: 0);
	for (<ml,n> <- methodsLOC) { // ml unused atm
		if (n > 0) {
			if (n <= 10) {
				amountPerCategory[1] += n;
			} else if (n <= 50) {
				amountPerCategory[2] += n;
			} else if (n <= 100) {
				amountPerCategory[3] += n;
			} else {
				// It's above 100!
				amountPerCategory[4] += n;
			}
		} else {
			// We had a parse error here
			;
		}
	}
	iprintln(amountPerCategory);
	
	// Now we know the LOC per category, we can define the percentages.
	
	
	
	
}