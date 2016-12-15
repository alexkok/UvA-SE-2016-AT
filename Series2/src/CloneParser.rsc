module CloneParser

import IO;
import List;
import String;
import Set;
import Relation;
import analysis::graphs::Graph;
import Config;
import Map;

data jsonDataTree = leaf(str name)
				  | folder(str name, list[jsonDataTree] subTree)
				  ;

public str clonesToJson(set[tuple[list[loc stmntLoc] locations, loc fullLoc] clone] clones) {
	relations = {};
	map[list[str],
		map[str, 
			list[tuple[loc, int]]
			]
		] filesData = ();

	for (location <- sort([fullLoc | <_,fullLoc> <- clones])) {
		path = tail(split("/", location.path));
		name = last(path);
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
	iprintln(filesData);
	relations += {<"program", r> | r <- top(relations)};
	str jsonResult = generateJsonObject(["program"], relations, filesData);
	println(jsonResult);
	writeFile(RESULT_JSON_FILE, jsonResult);
	return jsonResult;
}

private rel[str, str] getRelations(list[str] folders) {
	if (size(folders) > 1) 
		return <folders[0], folders[1]> + getRelations(tail(folders));
	return {};
}

private str generateJsonObject(path, relations, filesData) {
	// Get top
	// For all clones found in dataObjects[topkey] 
	// 	Gen template
	// For all references from top
	// 	Gen template again
	curName = last(path);
	children = "";
	files = "";
	shouldGenChildren = size(relations[curName]) > 0;
	shouldGenFiles = filesData[path]?;
	if (size(path) > 4)
	println();
	println("- Rels : <relations[curName]>");
	if (shouldGenFiles)
	println("- Files: <filesData[path]>");

	if (shouldGenChildren) {
		//children = genChildren();
		;
	}
	if (shouldGenFiles) {
		//files = genFiles();
		;
	}
	if (shouldGenChildren || shouldGenFiles) {
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
	} else {
		return "_ERROR_";
	}
	
		    //'    \"children\": [<for (i <- toList(relations[curName])) {>
		    //'        <generateJsonObject(i, relations, filesData)><
		    //		 if (i != last(toList(relations[curName]))) {>,<
		    //		 }><
	    	//	 }>
	
	//} else {
	//	return "{\"name\": \"<curName>\", \"size\": 4000}";
	//}
	
    //'  <for (x <- sort([f | f <- fields])) {>
    //'  private <fields[x]> <x>;
    //'  <genSetter(fields, x)>
    //'  <genGetter(fields, x)><}>
    //'}";
}

private str genChildren(curName, path, relations, filesData) {
	return
		"<for (i <- toList(relations[curName])) {>
		'<generateJsonObject(path + i, relations, filesData)><
		if (i != last(toList(relations[curName]))) {>,<}><
		}>";
}

private str genFiles(path, relations, filesData) {
println(size(filesData[path]));
	int  i = 0;
	return 
		"<for (key <- filesData[path]) { i+= 1;>
		'{\"name\": \"<key>\", \"size\": <(0 | it + n | <_,n> <- filesData[path][key])>}<
		if (i != size(filesData[path])) {>,<}><
		}>";
}