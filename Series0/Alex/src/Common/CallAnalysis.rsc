module Common::CallAnalysis

import Set;
import Relation;
import analysis::graphs::Graph;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */

// Define our own data type, which is basically an alias for a string
alias Proc = str;

public rel[Proc, Proc] calls = {<"a", "b">, <"b", "c">, <"b", "d">, <"d", "c">,  
						 <"d", "e">, <"f", "e">, <"f", "g">, <"g", "e">};

// Size of a relation
// > size(calls);

// All unique elements in the relation
// > carrier(calls);

// Then we have the domain and range functions. Taking all first elements and taking all second elements respectively.
// > domain(calls);
// > range(calls);

// We can also find out which points are calling others, but aren't called by by others
// > top(calls);

// Similarly, we can find out the leaves. Those that are only being called by others, but don't make calls theirselves.
// > bottom(calls);

// We can easily find out the transitive closure of a relation by using the + sign.
// > calls+
public rel[Proc, Proc] closureCalls = calls+;

// We can now check which points are calling which other points
// > closureCalls["a"]
// > closureCalls["f"]
public set[Proc] calledFromA = closureCalls["a"];
public set[Proc] calledFromF = closureCalls["f"];

// Then we can take the intersection of those both relations
// > calledFromA & calledFromF

// All of the above can be in a one liner:
// > (carrier(calls) | it & (calls+)[p] | p <- top(calls));
// The explanation: 
// 		The reducer is initialized with all procedures (carrier(Calls)) and iterates over all entry points (p <- top(Calls)). 
// 		At each iteration the current value of the reducer (it) is intersected (&) 
// 			with the procedures called directly or indirectly from that entry point ((Calls+)[p]).

