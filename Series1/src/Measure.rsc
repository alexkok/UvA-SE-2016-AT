module Measure

import Prelude;
import util::Webserver;
import IO;
import metrics::UnitSize;
import MetricsUtil;

loc head = |project://MetricsAnalyzer/src/ui/head.txt|;
loc body = |project://MetricsAnalyzer/src/ui/body.txt|;
loc body2 = |project://MetricsAnalyzer/src/ui/body2.txt|;

public str htmlFile(loc project) {
	list[str] lines = readFileLines(project);
	return ("" | it + l | l <- lines);
}

public void server(loc local, int metricAnalysability, int metricChangeability, int metricTestability, int metricMaintainability) {
	str html = "\<!DOCTYPE html\>
		\<html lang=\"en\"\>
			" + htmlFile(head) +"
			" + htmlFile(body) +"
			
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-user\"\>\</i\> Maintainability\</span\>              
				" + convertResultUI(metricMaintainability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Analysability\</span\>              
				" + convertResultUI(metricAnalysability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-user\"\>\</i\> Changeability\</span\>              
				" + convertResultUI(metricChangeability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-user\"\>\</i\> Testability\</span\>              
				" + convertResultUI(metricTestability) + "            
			\</div\>
			
			" + htmlFile(body2) +"
			
		\</html\>";

	//str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	str myFunction(map[str,str] params) = html;
	//serve(|http://localhost:8091|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	serve(local, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}