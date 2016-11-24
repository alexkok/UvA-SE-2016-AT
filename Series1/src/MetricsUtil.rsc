module MetricsUtil

import ParseTree;
import lang::java::\syntax::Java15;
import util::LOC;
import IO;
import String;

public tuple[str source, int extraVolumeLoc] createBigFile(set[loc] files,bool isDebug) {
	int i = 0;
	str result = "";
	int extraLines = 0;
	bool inMultiComment = false;
	for (l <- [trim(l) | f <- files, 
						 l <- readFileLines(f), 
						 !isEmpty(trim(l)), 
						 !startsWith(trim(l), "//") // Remove lines with only comments immediately
						 ]) {
		if (trim(l) == "}" ||  // Remove lines that only consist of a "}" | Lines with only a "}" should be taken into account for volume, ignored for duplication
			startsWith(trim(l), "package") || // Remove package lines | Package lines should be taken into account for volume, ignored for duplication
			startsWith(trim(l), "import") // Remove import lines | Import lines should be taken into account for volume, ignored for duplication 
			) {
			extraLines += 1;
		} else {
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
				
				if (contains(trim(l), "*/")) {
					inMultiComment = false;
				}
			} else if (inMultiComment && contains(l, "*/")) {
				inMultiComment = false;
				if (size(split(l, "\\*/")) > 1 && contains(split("\\*/")[1], "/*")) {
					// Closing, but also starting another multi comment
					inMultiComment = true;
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
		}
		i += 1;
	}
	return <result, extraLines>;
}

public int calculateLOC(type[&T<:Tree] theType, loc fragment, bool isDebug) {
	if (isDebug) print(".");
	try {
		//t = parse(theType, codeFragment);
		return countSLOC(parse(theType, fragment));
	} catch ParseError(l): {
		if (isDebug) println("\nFound a parse error in <l> at line <l.begin.line>, column <l.begin.column>");
		if (isDebug) println("	Method: <readFile(fragment)>");
		return 0;
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

public str convertResultUIStars(int result) {
	switch (result) {
		case 5: return "\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>";
		case 4: return "\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>" + "\<i class=\"fa fa-star-o\"\>\</i\>";
		case 3: return "\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>" + "\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>";
		case 2: return "\<i class=\"fa fa-star\"\>\</i\>\<i class=\"fa fa-star\"\>\</i\>" + "\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>";
		case 1: return "\<i class=\"fa fa-star\"\>\</i\>" + "\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>\<i class=\"fa fa-star-o\"\>\</i\>";
		default: return "N/A";
	}
}