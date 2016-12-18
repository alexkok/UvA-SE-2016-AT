module CloneParser

import IO;
import List;
import String;
import DateTime;
import Set;
import Relation;
import analysis::graphs::Graph;
import Config;
import Map;

public str parseClonesToJson(set[tuple[list[loc stmntLoc] locations, loc fullLoc] clone] clones, datetime startTime, datetime endTime) {
	relations = {};
	map[list[str],
		map[str, 
			list[tuple[loc, int]]
			]
		] filesData = ();

	for (location <- sort([fullLoc | <_,fullLoc> <- clones])) {
		path = tail(split("/", location.path));
		name = location.file;
		folders = prefix(path);
		relations += getRelations(folders);
		fullFolder = ["program"] + folders;
		if (filesData[fullFolder]?) {
			if (filesData[fullFolder][name]?) {
				filesData[fullFolder][name] += <location, size(readFileLines(location))>;
			} else {
				filesData[fullFolder] += (name : [<location, size(readFileLines(location))>]);
			}
		} else {
			filesData += (fullFolder : (name : [<location, size(readFileLines(location))>]));
		}
	}
	relations += {<"program", r> | r <- top(relations)};
	str jsonResult = generateJsonObject(["program"], relations, filesData);
	//println(jsonResult);
	writeFile(RESULT_JSON_LOCATION + "flare.json", jsonResult);
	str jsonDetailResult = generateJsonResultDetails(clones, filesData, startTime, endTime);
	writeFile(RESULT_JSON_LOCATION + "details.json", jsonDetailResult);
	//println(jsonDetailResult);
	return jsonResult;
}

private str generateJsonResultDetails(set[tuple[list[loc stmntLoc] locations, loc fullLoc] clone] clones, filesData, datetime startTime, datetime endTime) {
	map[str path, tuple[int dupLines, str content] fileData] detailsData = ();
	for (path <- filesData) {
		for (file <- filesData[path]) {
			fullPath = ("" | it + e + "/" | e <- tail(path)) + file;
			dupLines = (0 | it + e | <_, e> <- filesData[path][file]);
			clonesInFile = sort(filesData[path][file], bool(a, b){ return a[1] > b[1]; });
			str fileContent = 
				"{
				'  \"path\": \"<fullPath>\",
				'  \"loc_duplicate\": <dupLines>,
				'  \"clones\": [
				'      <for (<cloneLoc, cloneSize> <- clonesInFile) {cloneContent = escapeContent(cloneLoc);>{
				'      \"total_lines\": <size(readFileLines(cloneLoc))>,
				'      \"line_start\": <cloneLoc.begin.line>,
				'      \"line_end\": <cloneLoc.end.line>,
				'      \"content\": \"<cloneContent>\"
				'    }<
					if (<cloneLoc, cloneSize> != last(clonesInFile)) {>,<}>
				'    <}>
				'  ]
				'}";
			if (detailsData[fullPath]?) {
				detailsData[fullPath] += <dupLines, fileContent>; 
			} else {
				detailsData += (fullPath : <dupLines, fileContent>); 
			}
		}
	}
	//iprintln(detailsData);

	theDuration = createDuration(startTime, endTime);
	str result = "{
	'  \"time_start\": \"<printTime(startTime, "HH:mm:ss")>\",
	'  \"time_end\": \"<printTime(endTime, "HH:mm:ss")>\",
	'  \"total_duration\": \"<theDuration.hours>h <theDuration.minutes>m <theDuration.seconds>s\",
	'  \"loc_total\": 0,
	'  \"loc_duplicate\": <(0 | it + e | <_,<e, _>> <-toList(detailsData))>,
	'  \"total_clones\": <size(clones)>,
	'  \"clones\": [<
		for (<path,<dupLines, content>> <- toList(detailsData)) {>
	'    <content><
		if (<path,<dupLines, content>> != last(toList(detailsData))) {>,<}><}>
	'  ]
	'}";
	
	return result;
}

private str escapeContent(loc location) {
	escapeChars = replaceAll(readFile(location), "\\", "\\\\");
	escapeChars2 = replaceAll(escapeChars, "\"", "\\\"");
	escapeTabs = replaceAll(escapeChars2, "\t", "&nbsp;&nbsp;&nbsp;&nbsp;"); // Tabs to 4 html non-breaking spaces
	escapeNewlines = replaceAll(escapeTabs, "\r\n", "\<br/\>"); 
	return escapeNewlines;
}

private rel[str, str] getRelations(list[str] folders) {
	if (size(folders) > 1) 
		return <folders[0], folders[1]> + getRelations(tail(folders));
	return {};
}

private str generateJsonObject(path, relations, filesData) {
	curName = last(path);
	shouldGenChildren = size(relations[curName]) > 0;
	shouldGenFiles = filesData[path]?;
	return 
	    "{
	    '    \"name\": \"<curName>\",
	    '    \"children\": [<
	    	 if (shouldGenChildren) {>
	    '        <genChildren(curName, path, relations, filesData)><
	    if (shouldGenFiles) {>,<}>
	    '    <}><
	    	 if (shouldGenFiles) {>
	    '        <genFiles(path, relations, filesData)><}>
	    '    ]
	    '}"
	    ;
}

private str genChildren(curName, path, relations, filesData) {
	return
		"<for (i <- toList(relations[curName])) {>
		'<generateJsonObject(path + i, relations, filesData)><
		if (i != last(toList(relations[curName]))) {>,<}><
		}>";
}

private str genFiles(path, relations, filesData) {
	int  i = 0;
	return 
		"<for (key <- filesData[path]) { i+= 1;>
		'{\"name\": \"<("" | it + s + "/" | s <- path) + key>\", \"size\": <(0 | it + n | <_,n> <- filesData[path][key])>}<
		if (i != size(filesData[path])) {>,<}><
		}>";
}