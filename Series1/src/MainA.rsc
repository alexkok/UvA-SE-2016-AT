module MainA

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah (_STNUMMER_)
 * thanus.tharumarajah@student.uva.nl
 */

import IO;
//import List;
import Set;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import lang::java::m3::AST;
import ParseTree;
import util::LOC;
import lang::java::\syntax::Java15;

public loc prLoc = |project://MetricsTests2/src|;
public M3 m = createM3FromEclipseProject(prLoc);
//public M3 m = createM3FromEclipseProject(|project://MetricsTests2|);
public Declaration d = getMethodASTEclipse(|java+method:///main/Main/main(java.lang.String%5B%5D)|, model=m);

public void main() {
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
	t = parse(#CompilationUnit, theLocation);
	countSLOC(t);
}

public int calculateSomeLOC(loc location) {
	t = parse(#CompilationUnit, location);
	countSLOC(t);
	return countSLOC(t);
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
public void calculateVolume() {

}