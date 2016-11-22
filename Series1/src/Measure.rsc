module Measure

import Prelude;
import util::Webserver;
import IO;
import metrics::UnitSize;

public str head() {
	list[str] lines = readFileLines(|project://MetricsAnalyzer/src/ui/head.txt|);
	return ("" | it + l | l <- lines);
}

public str body() {
	list[str] lines = readFileLines(|project://MetricsAnalyzer/src/ui/body.txt|);
	return ("" | it + l | l <- lines);
}

public void server(loc local) {
	str html = "\<!DOCTYPE html\>
		\<html lang=\"en\"\>
			" + head() +"
			" + body() +"
		\</html\>";

	//str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	str myFunction(map[str,str] params) = html;
	//serve(|http://localhost:8091|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	serve(local, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}