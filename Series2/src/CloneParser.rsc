module CloneParser

import IO;
import List;
import String;
import Set;
import Relation;
import analysis::graphs::Graph;
import Config;

data jsonDataTree = leaf(str name)
				  | folder(str name, list[jsonDataTree] subTree)
				  ;

public str clonesToJson(set[tuple[list[loc stmntLoc] locations, loc fullLoc] clone] clones) {
	relations = {};
	filesData = ();
	for (location <- sort([fullLoc | <_,fullLoc> <- clones])) {
		path = tail(split("/", location.path));
		folders = prefix(path);
		relations += getRelations(folders);
		filesData += (["program"] + folders : <location, size(readFileLines(location))>);
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
		    '    \"name\": \"<curName>\"<
		    	 if (shouldGenChildren) {>,
		    '    \"children\": [
		    '        <genChildren(curName, path, relations, filesData)>
		    '    ]<
		    	 }><
		    	 if (shouldGenFiles) {>,
		    '    {<path>}<}>
		    '}"
		    ;
	} else {
		return "_<path>_";
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