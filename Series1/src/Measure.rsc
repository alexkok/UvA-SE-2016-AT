module Measure

import Prelude;
import util::Webserver;
import metrics::UnitSize;

public void server() {
	str html = " \<!DOCTYPE html\>
\<html\>
\<head\>
\<title\>Page Title\</title\>
\</head\>
\<body\>

\<h1\>This is a Heading\</h1\>
\<p\>This is a paragraph.\</p\>

\</body\>
\</html\>";

	//str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	str myFunction(map[str,str] params) = html;
	serve(|http://localhost:8090|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}