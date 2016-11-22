module Main

import Set;
import DateTime;
import IO;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;

// Our metrics modules
import MetricsUtil;
import metrics::Volume;
import metrics::UnitSize;
import metrics::UnitComplexity;
import metrics::Duplication;

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

// Values to keep track of the analysis of the current project
private datetime analysisStartTime, analysisEndTime;
private M3 projectM3Model;
private str bigFileOfProject;
private int metricTotalVolume;
private int metricVolumeResult;
private list[tuple[loc, int, int]] metricTotalUnitSize;
private int metricUnitSizeResult;
private list[tuple[loc, int, int]] metricTotalUnitComplexity;
private int metricUnitComplexityResult;


/**
 * The main method.
 * Starting the analyzer and computing each metric on the given project.
 */
public void main(loc projectLocation = projectLocations[1]) {
	analysisStartTime = now();
	println("*************** Metrics Analyzer ***************");
	println("* Alex Kok                                     *");
	println("* Thanusijan Tharumarajah                      *");
	println("*                                              *");
	println("* TODOS:                                       *");
	println("* - Can improve volume by just grabbing the big*");
	println("*   file and count the \\r\\n lines. (But we     *");
	println("*   shouldn\'t remove package and import       *");
	println("*   statements in this case then)              *");
	println("************************************************");
	println("- Start time:\t\t <printDateTime(analysisStartTime)>");
	println("- Project location:\t <projectLocation>");
	println("- Metrics to calculate:\t Volume, Unit Size, Unity Complexity and Duplication");
	println("- Tables used to compute the metrics can be found in the source. Those tables will also be printed here in the meantime when the result is being computed for a specific metric.");
	println();
	
	println("** Phase 1: Preparing project data");
	print("- Progress: ");
	projectM3Model = createM3FromEclipseProject(projectLocation);
	bigFileOfProject = createBigFile(files(projectM3Model));
	println("\n- Files: <size(files(projectM3Model))>");
	println("- Methods: <size(methods(projectM3Model))>");
	println();
	
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
	println("\> Metric table (Source: SIG):");
	println("\> --------------------------------");
	println("\> Rank | Man years  | KLOC in Java");
	println("\> --------------------------------");
	println("\>  + + |   0 - 8    |    0 - 66");
	println("\>   +  |   8 - 30   |   66 - 246");
	println("\>   0  |  30 - 80   |  245 - 665");
	println("\>   -  |  80 - 160  |  655 - 1310");
	println("\>  - - |   \> 160    |    \> 1310");
	println("- Resulting in:\t <metricVolumeResult>");
	println();
	
	println("** Phase 3: Calculating metric: Unit Size");
	metricTotalUnitSize = calculateUnitSize(projectM3Model);
	metricUnitSizeResult = calculateUnitSizeResult(metricTotalUnitSize, metricTotalVolume);
	print("- Progress: ");
	println("\n- The LOC of each method will be categorized in the following categories:");
	println("\> Metric table (Source: http://docs.sonarqube.org/display/SONARQUBE45/SIG+Maintainability+Model+Plugin):");
	println("\> ----------------");
	println("\> Category |  LOC");
	println("\> ----------------");
	println("\> Very high| \> 100 ");
	println("\> High     | \> 50");
	println("\> Medium   | \> 10");
	println("\> Low      | \> 0");
	println("- Each category will be compared to the following table to compute the result:");
	println("\> Metric table (Source: SIG):");
	println("\> --------- Maximum relative LOC ---");
	println("\> Rank | Moderate | High | Very high ");
	println("\> ----------------------------------");
	println("\>  + + |   25%    |  0%  |   0%");
	println("\>   +  |   30%    |  5%  |   0%");
	println("\>   0  |   40%    | 10%  |   0%");
	println("\>   -  |   50%    | 15%  |   5%");
	println("\>  - - |    -     |  -   |    -");
	println("- The total LOC that is used here is the Volume calculated earlier.");
	println("- Resulting in:\t <metricUnitSizeResult>");
	println();
	
	println("** Phase 4: Calculating metric: Unit Complexity");
	metricTotalUnitComplexity = calculateUnitComplexity(metricTotalUnitSize, projectM3Model);
	metricUnitComplexityResult = calculateUnitComplexityResult(metricTotalUnitComplexity, metricTotalVolume);
	println("\n- The LOC of each method will be categorized in the following categories:");
	println("\> Metric table (Source: SIG):");
	println("\> ----------------");
	println("\> Category |  LOC");
	println("\> ----------------");
	println("\> Very high| \> 50 ");
	println("\> High     | 21-50");
	println("\> Medium   | 11-20");
	println("\> Low      |  1-10");
	println("- Each category will be compared to the following table to compute the result:");
	println("\> Metric table (Source: SIG):");
	println("\> --------- Maximum relative LOC ---");
	println("\> Rank | Moderate | High | Very high ");
	println("\> ----------------------------------");
	println("\>  + + |   25%    |  0%  |   0%");
	println("\>   +  |   30%    |  5%  |   0%");
	println("\>   0  |   40%    | 10%  |   0%");
	println("\>   -  |   50%    | 15%  |   5%");
	println("\>  - - |    -     |  -   |    -");
	println("- The total LOC that is used here is the Volume calculated earlier.");
	println("- Resulting in:\t <metricUnitComplexityResult>");
	println();
	
	println("** Phase 5: Calculating metric: Duplication");
	print("- Progress: ");
	metricDuplications = findDuplications(bigFileOfProject);
	print("- Duplicated lines found. Calculating blocks...");
	print("- Progress: ");
	metricDuplicationBlocks = calculateDuplicationBlocks(metricDuplications);
	metricDuplicationsTotalLines = (0 | it + n | <_,n> <- metricDuplicationBlocks);
	metricDuplicationResult = calculateDuplicationResult(metricDuplicationsTotalLines, metricTotalVolume);
	println("\n- Resulting in:\t <metricDuplicationResult>");
	println();
	
	println("** Result");
	println("|--------------------------------|");
	println("| Metric \t\tResult\t | Extra comment");
	println("|--------------------------------|");
	println("\> Volume: \t\t <metricVolumeResult> \t | LOC: <metricTotalVolume>");
	println("\> Unit Size: \t\t <metricUnitSizeResult> \t |");
	println("\> Unit Complexity: \t <metricUnitComplexityResult> \t |");
	println("\> Duplication: \t\t <metricDuplicationResult> \t | DLOC: <metricDuplicationsTotalLines>");
	analysisEndTime = now();
	println();
	
	println("- End time: <printDateTime(analysisEndTime)>");
	println("- Analysis duration (y,m,d,h,m,s,ms): <createDuration(analysisStartTime, analysisEndTime)>");
}