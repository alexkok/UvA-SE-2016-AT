module MetricsUtil

import ParseTree;
import lang::java::\syntax::Java15;
import util::LOC;
import IO;
import String;

public str createBigFile(set[loc] files, bool isDebug) {
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
		if (isDebug) println("[<i>] <l>");
		//if (isDebug) print(".");
		i += 1;
	}
	return result;
}

public int calculateLOC(type[&T<:Tree] theType, str codeFragment, bool isDebug) {
	if (isDebug) print(".");
	if (startsWith(codeFragment, "/*")) {
		codeFragment = trim(split("*/", codeFragment)[1]);
	}
	try {
		t = parse(theType, codeFragment);
		return countSLOC(t);
	} catch ParseError(l): {
		if (isDebug) println("\nFound a parse error in <l> at line <l.begin.line>, column <l.begin.column>");
		if (isDebug) println("	Method: <codeFragment>");
		return -1;
	}
}

public str convertResult(int result) {
	switch (result) {
		case 5: return "++";
		case 4: return "+";
		case 3: return "0";
		case 2: return "-";
		case 1: return "--";
		default: return "N/A";
	}
}

public str convertResultStars(int result) {
	switch (result) {
		case 5: return "\u2605\u2605\u2605\u2605\u2605";
		case 4: return "\u2605\u2605\u2605\u2605" + "\u2606";
		case 3: return "\u2605\u2605\u2605" + "\u2606\u2606";
		case 2: return "\u2605\u2605" + "\u2606\u2606\u2606";
		case 1: return "\u2605" + "\u2606\u2606\u2606\u2606";
		default: return "N/A";
	}
}

public str convertResultUI(int result) {
	switch (result) {
		case 5: return "\<div class=\"count green\"\>" + convertResult(result) + "\</div\>";
		case 4: return "\<div class=\"count green\"\>" + convertResult(result) + "\</div\>";
		case 3: return "\<div class=\"count\"\>" + convertResult(result) + "\</div\>";
		case 2: return "\<div class=\"count red\"\>" + convertResult(result) + "\</div\>";
		case 1: return "\<div class=\"count red\"\>" + convertResult(result) + "\</div\>";
		default: return "N/A";
	}
}