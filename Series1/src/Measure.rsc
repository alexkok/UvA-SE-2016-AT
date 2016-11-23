module Measure

import Prelude;
import util::Webserver;
import IO;
import metrics::UnitSize;
import MetricsUtil;

loc head = |project://MetricsAnalyzer/src/ui/head.txt|;
loc body = |project://MetricsAnalyzer/src/ui/body.txt|;
loc body2 = |project://MetricsAnalyzer/src/ui/body2.txt|;
loc body3 = |project://MetricsAnalyzer/src/ui/body3.txt|;

public str htmlFile(loc project) {
	list[str] lines = readFileLines(project);
	return ("" | it + l | l <- lines);
}

public void server(loc local, int metricAnalysability, int metricChangeability, int metricTestability, int metricMaintainability, 
					int metricDuplicationsTotalLines, int metricTotalVolume) {
	str html = "\<!DOCTYPE html\>
		\<html lang=\"en\"\>
			" + htmlFile(head) +"
			" + htmlFile(body) +"
			
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Maintainability\</span\>              
				" + convertResultUI(metricMaintainability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Analysability\</span\>              
				" + convertResultUI(metricAnalysability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Changeability\</span\>              
				" + convertResultUI(metricChangeability) + "            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Testability\</span\>              
				" + convertResultUI(metricTestability) + "            
			\</div\>
			
			" + htmlFile(body2) +"
			
			\<div class=\"widget_summary\"\>                    
				\<div class=\"w_left w_25\"\>                      
					\<span\>LOC\</span\>                    
				\</div\>                    
				\<div class=\"w_center w_55\"\>                      
					\<div class=\"progress\"\>                        
						\<div class=\"progress-bar bg-green\" role=\"progressbar\" aria-valuenow=\"60\" aria-valuemin=\"0\" aria-valuemax=\"100\" 
							style=\"width: " + toString(metricTotalVolume * 100 / 1310000) + "%;\"\>                          
							\<span class=\"sr-only\"\>60% Complete\</span\>                        
						\</div\>                      
					\</div\>                    
				\</div\>                    
				\<div class=\"w_right w_20\"\>                      
					\<span\>" + toString(metricTotalVolume) + "\</span\>                    
				\</div\>                    
				\<div class=\"clearfix\"\>\</div\>                  
			\</div\>
			                  
			\<div class=\"widget_summary\"\>                    
				\<div class=\"w_left w_25\"\>                      
					\<span\>DLOC\</span\>                    
				\</div\>                    
				\<div class=\"w_center w_55\"\>                      
					\<div class=\"progress\"\>                        
						\<div class=\"progress-bar bg-green\" role=\"progressbar\" aria-valuenow=\"60\" aria-valuemin=\"0\" aria-valuemax=\"100\" 
							style=\"width: " + toString(metricDuplicationsTotalLines * 100 / metricTotalVolume) + "%;\"\>                          
							\<span class=\"sr-only\"\>60% Complete\</span\>                        
						\</div\>                      
					\</div\>                    
				\</div\>                    
				\<div class=\"w_right w_20\"\>                      
					\<span\>" + toString(metricDuplicationsTotalLines) + "\</span\>                    
				\</div\>                    
				\<div class=\"clearfix\"\>\</div\>                  
			\</div\>
			
			" + htmlFile(body3) +"
			
		\</html\>";

	//str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	str myFunction(map[str,str] params) = html;
	//serve(|http://localhost:8091|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	serve(local, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}