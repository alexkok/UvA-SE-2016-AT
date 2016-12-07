module Main

import Prelude;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import ParseTree;
import vis::ParseTree;
import lang::java::\syntax::Java15;

private map[int, str] clones;

public void main() {
	loc project = |project://MetricsTests2/src|;
	projectM3Model = createM3FromEclipseProject(project);
	
	//set[Declaration] asts = createAstsFromEclipseProject(project, true);
	Declaration asts = createAstsFromEclipseFile(|project://MetricsTests2/src/main/MainA.java|, true);
	
	//subtree
	visit(asts) {
		case a: a;
	}
}

public int sizeNodes(Declaration ast) {
	int count = 0;
	
	visit(ast) {
		case node n: {
			count += 1;
		}
	}
	
	return count;
}

public int bucketSize(int nodeSize) {
	return 10 % nodeSize;
}

//loc file = |file:///Users/Thanus/Documents/informatica/master/software%20evolution/UvA-SE-2016-AT/MetricProjects/MetricsTests2/src/main/MainA.java|;
//	
//	try {
//		Tree t = parse(#start[CompilationUnit], file);
//		
//		visit(t) {
//			case ClassMod e: println("<e>");
//		}
//		
//		//renderParsetree(t);
//		//iprintln(t);
//} catch ParseError(l): {
//		println("\nFound a parse error in <l> at line <l.begin.line>, column <l.begin.column>");
//	}
