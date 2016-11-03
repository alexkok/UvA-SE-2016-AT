module Basic::Quine

import IO;
import String;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
public void quine() {
	println(program);
	println("\"" + escape(program, ("\"" : "\\\"", "\\" : "\\\\")) + "\";");
}

str program = 
"module demo::basic::Quine

import IO;
import String;

public void quine(){
  println(program);
  println(\"\\\"\" + escape(program, (\"\\\"\" : \"\\\\\\\"\", \"\\\\\" : \"\\\\\\\\\")) + \"\\\";\");
}

str program =";