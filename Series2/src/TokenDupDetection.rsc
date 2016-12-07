module TokenDupDetection

import ParseTree;
import vis::ParseTree;
import lang::java::\syntax::Java15;
import IO;
import String;
import List;
import Map;

public loc fileLoc = |project://MetricsTests2/src/main/MainA.java|; 

public void parseSomeTree() {
	Tree aTree = parse(#CompilationUnit, fileLoc);
	lines = readFileLines(fileLoc);
	int fileSize = size(lines);
	//renderParsetree(aTree);
	//return;
	map[int, int] linesSize = (ln : size(lines[ln]) | ln <- [0..size(lines)]);
	//iprintln(aTree); // To show it parses succesfully
	println("- Lines of file: <fileSize>");
	//println("- Map: <linesSize>");
	//println("- Map0: <linesSize[26]>");
	iprintln(sort(toList(linesSize)));
	
	//if (/<word:\W+>/ := readFile(fileLoc)) {
	//	;
	//	println("-  Found word: <word>");
	//}
	//iprintln(aTree);
	
	//iprintln(aTree@links);
	//return;
	visit (aTree) {
		case CompilationUnit a: println(a);
		//case c:appl(a,b) : 
		//	try {
		//		loc aLoc = c@\loc;
		//		println("-  Token: <c@\loc> | <a>");
		//	} catch NoSuchAnnotation (e) : {
		//		// Happens on specific chars, like (, ), { and } that are not used as lexicals
		//		//println(c);
		//		;
		//	}
		//case c:appl(prod(sort(a),e,d),b) :
		//	try { 
		//		if (c@\loc.begin.line == c@\loc.end.line) {
		//			println("<c@\loc> | <c> | <a> | <b>");
		//			println();
		//		} 
		//	} catch NoSuchAnnotation (e) : {
		//		println(c);
		//	}
		//case t:sort(s):
		//	try {
		//		loc aLoc = t@\loc; 
		//		println("Token: <s> | <aLoc>");
		//	} catch NoSuchAnnotation (e) : {
		//		;
		//	}
		//case a:appl(_,_): 
		//	visit(a) {
		//		case b:sort("Id"):
		//			try {
		//				loc aLoc = a@\loc; 
		//				println("Type: | <aLoc> | Id");
		//			} catch NoSuchAnnotation (e) : {
		//				;
		//			}
		//	}
		
		case a:TypeName:
			try {
				//loc aLoc = a@\loc; 
				//println("Type: | <aLoc> | Id");
				println("<a>");
			} catch NoSuchAnnotation (e) : {
				;
			}
		default: print("");
	}
	
	//renderParsetree(aTree); // This creates the IndexOutOfBounds exception
}