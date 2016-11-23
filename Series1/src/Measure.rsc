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
loc body4 = |project://MetricsAnalyzer/src/ui/body4.txt|;
loc body5 = |project://MetricsAnalyzer/src/ui/body5.txt|;
loc body6 = |project://MetricsAnalyzer/src/ui/body6.txt|;

public str htmlFile(loc project) {
	list[str] lines = readFileLines(project);
	return ("" | it + l | l <- lines);
}

public void server(loc local, int metricAnalysability, int metricChangeability, int metricTestability, int metricMaintainability, 
					int metricDuplicationsTotalLines, int metricTotalVolume, map[int, int] metricUnitSizeCategories, map[int, int] metricUnitComplexityCategories) {
	str html = "\<!DOCTYPE html\>
		\<html lang=\"en\"\>
			<htmlFile(head)>
			<htmlFile(body)>
			
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Maintainability\</span\>              
				<convertResultUI(metricMaintainability)>            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Analysability\</span\>              
				<convertResultUI(metricAnalysability)>            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Changeability\</span\>              
				<convertResultUI(metricChangeability)>            
			\</div\>            
			\<div class=\"col-md-3 col-sm-4 col-xs-6 tile_stats_count\"\>              
				\<span class=\"count_top\"\>\<i class=\"fa fa-clock-o\"\>\</i\> Testability\</span\>              
				<convertResultUI(metricTestability)>            
			\</div\>
			
			<htmlFile(body2)>
			
			\<div class=\"widget_summary\"\>                    
				\<div class=\"w_left w_25\"\>                      
					\<span\>LOC\</span\>                    
				\</div\>                    
				\<div class=\"w_center w_55\"\>                      
					\<div class=\"progress\"\>                        
						\<div class=\"progress-bar bg-green\" role=\"progressbar\" aria-valuenow=\"60\" aria-valuemin=\"0\" aria-valuemax=\"100\" 
							style=\"width: <metricTotalVolume * 100 / 1310000>%;\"\>                          
							\<span class=\"sr-only\"\>60% Complete\</span\>                        
						\</div\>                      
					\</div\>                    
				\</div\>                    
				\<div class=\"w_right w_20\"\>                      
					\<span\><metricTotalVolume>\</span\>                    
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
							style=\"width: <metricDuplicationsTotalLines * 100 / metricTotalVolume>%;\"\>                          
							\<span class=\"sr-only\"\>60% Complete\</span\>                        
						\</div\>                      
					\</div\>                    
				\</div\>                    
				\<div class=\"w_right w_20\"\>                      
					\<span\><metricDuplicationsTotalLines>\</span\>                    
				\</div\>                    
				\<div class=\"clearfix\"\>\</div\>                  
			\</div\>
			
			<htmlFile(body3)>
			
			\<table class=\"\" style=\"width:100%\"\>                    
				\<tr\>                      
					\<th style=\"width:37%;\"\>                        
						\<p\>Top 5\</p\>                      
					\</th\>                      
					\<th\>                        
						\<div class=\"col-lg-7 col-md-7 col-sm-7 col-xs-7\"\>                          
							\<p class=\"\"\>Risk evaluation\</p\>                        
						\</div\>                        
						\<div class=\"col-lg-5 col-md-5 col-sm-5 col-xs-5\"\>                          
							\<p class=\"\"\>LOC\</p\>                        
						\</div\>                      
					\</th\>                    
				\</tr\>                    
				\<tr\>                      
					\<td\>                        
						\<canvas id=\"canvas1\" height=\"140\" width=\"140\" style=\"margin: 15px 10px 10px 0\"\>\</canvas\>                      
					\</td\>                      
					\<td\>                        
						\<table class=\"tile_info\"\>                          
							\<tr\>                            
								\<td\>                              
									\<p\>\<i class=\"fa fa-square blue\"\>\</i\>Simple \</p\>                            
								\</td\>                            
								\<td\><metricUnitSizeCategories[1] * 100 / metricTotalVolume>%\</td\>                          
							\</tr\>                          
							\<tr\>                            
								\<td\>                              
									\<p\>\<i class=\"fa fa-square green\"\>\</i\>More complex \</p\>                            
								\</td\>                            
								\<td\><metricUnitSizeCategories[2] * 100 / metricTotalVolume>%\</td\>                          
							\</tr\>                          
							\<tr\>                            
								\<td\>                              
									\<p\>\<i class=\"fa fa-square purple\"\>\</i\>Complex \</p\>                            
								\</td\>                            
								\<td\><metricUnitSizeCategories[3] * 100 / metricTotalVolume>%\</td\>                          
							\</tr\>                          
							\<tr\>                            
								\<td\>                              
									\<p\>\<i class=\"fa fa-square aero\"\>\</i\>Untestable \</p\>                            
								\</td\>                            
								\<td\><metricUnitSizeCategories[4] * 100 / metricTotalVolume>%\</td\>                          
							\</tr\>                                           
						\</table\>                      
					\</td\>                    
				\</tr\>                  
			\</table\>                
		\</div\>              
	\</div\>            
\</div\>
			
			<htmlFile(body4)>
			
			var echartPieCollapse = echarts.init(document.getElementById(\'echart_pie2\'), theme);            
			echartPieCollapse.setOption({        
				tooltip: {          
					trigger: \'item\',          
					formatter: \"{a} \<br/\>{b} : {c} ({d}%)\"        
				},        
				legend: {          
					x: \'center\',          
					y: \'top\',          
					data: [\'Simple\', \'More complex\', \'Complex\', \'Untestable\']        
				},        
				toolbox: {          
					show: false,          
					feature: {            
						magicType: {              
							show: true,              
							type: [\'pie\', \'funnel\']            
						},            
						restore: {              
							show: true,              
							title: \"Restore\"            
						},            
						saveAsImage: {              
							show: true,              
							title: \"Save Image\"            
						}          
					}        
				},        
				calculable: true,        
				series: [{          
					name: \'Area Mode\',          
					type: \'pie\',          
					radius: [25, 60],          
					center: [\'50%\', 145],          
					roseType: \'area\',          
					x: \'50%\',          
					max: <metricTotalVolume>,          
					sort: \'ascending\',          
					data: [{            
						value: <metricUnitComplexityCategories[1]>,            
						name: \'Simple\'          
					}, {            
						value: <metricUnitComplexityCategories[2]>,            
						name: \'More complex\'          
					}, {            
						value: <metricUnitComplexityCategories[3]>,            
						name: \'Complex\'          
					}, {            
						value: <metricUnitComplexityCategories[4]>,            
						name: \'Untestable\'          
					}]        
				}]      
			});
			
			<htmlFile(body5)>
			
			\<!-- Doughnut Chart --\>    
				\<script\>      
					$(document).ready(function(){        
						var options = {          
							legend: false,          
							responsive: false        
					};        
					
					new Chart(document.getElementById(\"canvas1\"), {          
						type: \'doughnut\',          
						tooltipFillColor: \"rgba(51, 51, 51, 0.55)\",          
						data: {            
							labels: [              
								\"Simple\",              
								\"More complex\",              
								\"Complex\",              
								\"Untestable\",                    
							],            
							datasets: [{              
								data: [<metricUnitSizeCategories[1] * 100 / metricTotalVolume>, 
										<metricUnitSizeCategories[2] * 100 / metricTotalVolume>, 
										<metricUnitSizeCategories[3] * 100 / metricTotalVolume>, 
										<metricUnitSizeCategories[4] * 100 / metricTotalVolume>],              
								backgroundColor: [
								\"#3498DB\",                
								\"#26B99A\",                      
								\"#9B59B6\",
								\"#BDC3C7\"                        
							],              
							hoverBackgroundColor: [
								\"#49A9EA\",                
								\"#36CAAB\",                                    
								\"#B370CF\",
								\"#CFD4D8\"             
							 ]            
							}]          
						},          
						options: options        
						});      
					});    
				\</script\>    
			\<!-- /Doughnut Chart --\>
			
			<htmlFile(body6)>
		\</html\>";

	//str myFunction(map[str,str] params) = "\<div\>Hello Rascal\</div\>";
	str myFunction(map[str,str] params) = html;
	//serve(|http://localhost:8091|, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	serve(local, functionserver(("/index.html": myFunction, "/volume.html": myFunction)));
	//serve(|http://localhost:8092|, fileserver(|project://MetricsTests2/src/main/test.json|));
}