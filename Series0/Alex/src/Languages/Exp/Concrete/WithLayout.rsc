module Languages::Exp::Concrete::WithLayout

import String;
import ParseTree;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */

//layout Whitespace = [\t-\n\r\ ]*;
layout Whitespace = [\t-\n\r\ ]* !>> [\ \t\n\r]; // So we can remove ambiguity, Whitespaces should not be followed by Whitespaces  
    
lexical IntegerLiteral = [0-9]+;           

start syntax Exp 
  = IntegerLiteral          
  | bracket "(" Exp ")"     
  > left Exp "*" Exp        
  > left Exp "+" Exp        
  ;

public int eval(str txt) = eval(parse(#Exp, txt));              

public int eval((Exp)`<IntegerLiteral l>`) = toInt("<l>");       
public int eval((Exp)`<Exp e1> * <Exp e2>`) = eval(e1) * eval(e2);  
public int eval((Exp)`<Exp e1> + <Exp e2>`) = eval(e1) + eval(e2); 
public int eval((Exp)`( <Exp e> )`) = eval(e);                    

public value main(list[value] args) {
  return eval("2+3");
}

// Actualy, Rascal just adds the Whitespaces everywhere it can, ignoring those values anytime
// Rascal interprets it like this:
//syntax Exp 
//  = IntegerLiteral          
//  | bracket "(" Whitespace Exp Whitespace ")"     
//  > left Exp Whitespace "*" Whitespace Exp        
//  > left Exp Whitespace "+" Whitespace Exp        
//  ;
//
//syntax start[Exp] = Whitespace Exp top Whitespace;