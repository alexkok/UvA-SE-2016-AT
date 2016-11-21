module Measure

import Prelude;
import util::Webserver;
import metrics::UnitSize;

public void server() {
	str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	//serve(|http://localhost:8090|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}