module MetricsUtil

import ParseTree;
import lang::java::\syntax::Java15;
import util::LOC;
import IO;
import String;

public str createBigFile(set[loc] files) {
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
		//println("[<i>] <l>");
		print(".");
		i += 1;
	}
	return result;
}

public int calculateLOC(type[&T<:Tree] theType, loc location) {
	print(".");
	try {
		t = parse(theType, location);
		return countSLOC(t);
	} catch ParseError(loc l): {
		println("\nFound a parse error in <l> at line <l.begin.line>, column <l.begin.column>");
		return -1;
	}
}