module oldmetrics::UnitComplexity

import Prelude;
import ParseTree;
import util::LOC;
import lang::java::m3::Core;
import lang::java::m3::AST;
import lang::java::jdt::m3::AST;
import lang::java::jdt::m3::Core;
import lang::java::\syntax::Java15;


public void main() {
	startTime = now();
	println("Starting unit size analysis \n<printDateTime(startTime)>");
	
	//loc project = |project://MetricsTests2/src/main/|;
	//calculateUnitComplexity(project);
	
	endTime = now();
	println("<printDateTime(endTime)> \n<createDuration(startTime, endTime)>");
}

public void calculateUnitComplexity(loc project) {
	myModel = createM3FromEclipseProject(project);
	myMethods = methods(myModel);
	
	for(method <- myMethods) {
		methodAST = getMethodASTEclipse(method, model=myModel);
		int sumIf = 0, sumWhile = 0, sumFor = 0, sumCase = 0, sumCatch = 0, sumAnd = 0, sumOr = 0, sumConditional = 0, sumAssert = 0;
		int complexity = 1;
		
		visit(methodAST) {
			// count if / if else
			case \if(_,_) : sumIf += 1;
			case \if(_,_,_) : sumIf += 1;
			
			// count while
			case \while(_,_) : sumWhile += 1;
			
			// count for and foreach
			case \foreach(_,_,_) : sumFor += 1;
			case \for(_,_,_) : sumFor += 1;
			case \for(_,_,_,_) : sumFor += 1;
			
			// switch cases
			case \case(_) : sumCase += 1;
			
			// catch
			case \catch(_, _) : sumCatch += 1;
			
			// conditions
			case \infix(_,"&&",_) : sumAnd += 1;
			case \infix(_,"||",_) : sumOr += 1;
			
			// ternary
			case \conditional(_,_,_) : sumConditional += 1;
			
			// assert
			case \assert(_) : sumAssert += 1;
			case \assert(_, _) : sumAssert += 1;
		};
		
		complexity += sumIf + sumWhile + sumFor + sumCase + sumCatch + sumAnd + sumOr + sumConditional + sumAssert;
		
		println("if: <sumIf>");
		println("while: <sumWhile>");
		println("for: <sumFor>");
		println("case: <sumCase>");
		println("catch: <sumCatch>");
		println("and: <sumAnd>");
		println("or: <sumOr>");
		println("conditional: <sumConditional>");
		println("sumAssert: <sumAssert>");
		println(<method, complexity>);
	}
}