module Common::CallLifting

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
 alias proc = str;
 alias comp = str;
 
 public rel[proc, proc] theCalls = {<"main", "a">, <"main","b">, <"a","c">, <"a","b">, <"a","d">, <"b","d">};
 public rel[proc, comp] thePartsOf = {<"main", "Appl">, <"a","Appl">, <"b","DB">, <"c","Lib">, <"d","Lib">};
 
 public rel[comp, comp] lift(rel[proc, proc] calls, rel[proc, comp] aPartOf) {
 	return {<c1, c2> | <proc p1, proc p2> <- calls, <comp c1, comp c2> <- aPartOf[p1] * aPartOf[p2]};
 }