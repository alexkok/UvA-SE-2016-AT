module Common::CountInLine

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
 public int countInLine1(str s) {
 	int count = 0;
 	for (/[a-zA-Z0-9_]+/ := s) {
 		count += 1;
 	}
 	return count;
 }
 
 // > countInLine1("Jabberwocky by Lewis Carroll");
 
 public int countInLine2(str s) {
 	int count = 0;
 	
 	// \w matches any word character
 	// \W matches any non-word character
 	// <...> are groups and should appear at the top level.
 	while (/^\W*<word:\w+><rest:.*$>/ := s) {
 		// We can use the 'word' and 'rest' variable here now 
 		count += 1;
 		s = rest;
 		//println(word);
 	}
 	return count;
 }
 
 public int countInLine3(str s) {
 	return (0 | it + 1 | /\w+/ := s);
 }