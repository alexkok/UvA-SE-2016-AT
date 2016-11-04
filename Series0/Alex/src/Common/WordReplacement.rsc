module Common::WordReplacement

import String;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
 // Another version of capitalize
 public str capitalize(str s) {
 	if (/^<letter:[a-z]><rest:.*$>/ := s) {
 		return toUpperCase(letter) + rest;
 	} else {
 		return word;
	}
 }
 
 // Capitalize all words in a string
 
 // V1: using a while loop
 public str capAll1(str s) {
 	result = "";
 	while (/^<before:\W*><word:\w+><after:.*$>/ := s) {
 		result = result + before + capitalize(word);
 		s = after;
 	}
 	return result;
 }
 
 // > capAll1("turn this into a capitalized title")
 
 // V2: Using visit
 public str capAll2(str s) {
 	return visit(s) {
 		case /^<word:\w+>/i => capitalize(word) // What does the i here? it can be removed... 
 	};
 }