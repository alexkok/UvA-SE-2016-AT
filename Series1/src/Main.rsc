module Main

import Set;
import DateTime;
import IO;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;

// Our metrics modules
import metrics::Volume;

/**
 * Alex Kok
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah
 * thanus.tharumarajah@student.uva.nl
 */

// Made public so we can easily put those into the main() method in the console.
public list[loc] projectLocations = [
	|project://MetricsTests2/src|, // Our project with some defined tests
	|project://smallsql0.21_src/src|, // The SmallSQL project
	|project://hsqldb-2.3.1/hsqldp/src| // The hsqldb project
];

private M3 projectM3Model;

private int metricTotalVolume;
private str metricVolumeResult;

@doc{
	The main method. 
	Starting the analyzer and computing each metric on the given project.
}
public void main(loc projectLocation = projectLocations[0]) {
	startTime = now();
	println("*************** Metrics Analyzer ***************");
	println("* Alex Kok                                     *");
	println("* Thanusijan Tharumarajah                      *");
	println("*                                              *");
	println("* TODOS:                                       *");
	println("* - Volume table                               *");
	println("* - Other metrics                              *");
	println("* - Can improve volume by just grabbing the big*");
	println("*   file and count the \r\n lines. (But we     *");
	println("*   shouldn\'t remove package and import       *");
	println("*   statements in this case then)              *");
	println("************************************************");
	println("- Start time:\t\t <startTime>");
	println("- Project location:\t <projectLocation>");
	println("- Metrics to calculate:\t Volume, Unit Size, Unity Complexity and Duplication");
	println();
	
	println("** Phase 1: Preparing project data");
	prepareProjectData(projectLocation);
	println("- Files: <size(files(projectM3Model))>");
	println("- Methods: <size(methods(projectM3Model))>");
	
	println("** Phase 2: Calculating metric: Volume");
	println("- Will be computed based on the SIG Volume metric");
	println("  - A line consisting of only \"{\" or \"}\" will be considered as a LOC");
	println("  - Package statements will be considered as LOC");
	println("  - Import statements will be considered as LOC");
	println("  - Comments will NOT be considered as a LOC");
	println("  - Empty lines will NOT be considered as a LOC");
	print("- Progress: ");
	metricTotalVolume = calculateVolume(projectM3Model);
	metricVolumeResult = calculateVolumeResult(metricTotalVolume);
	println("\n- Volume:\t <metricTotalVolume>");
	println("- Resulting in:\t <metricVolumeResult>");
	
	println("** Phase 3: Calculating metric: Unit Size");
	println("- TODO");
	println("- Resulting in:\t __");
	
	println("** Phase 4: Calculating metric: Unit Complexity");
	println("- TODO");
	println("- Resulting in:\t __");
	
	println("** Phase 5: Calculating metric: Duplication");
	println("- TODO");
	println("- Resulting in:\t __");
	
	println("** Result");
	println("----------------------------------------------");
	println("  Metric \t\t | Values causing this");
	println("----------------------------------------------");
	println("- Volume: <metricVolumeResult> \t\t | LOC: <metricTotalVolume>");
}

private void prepareProjectData(loc project) {
	projectM3Model = createM3FromEclipseProject(project);
}