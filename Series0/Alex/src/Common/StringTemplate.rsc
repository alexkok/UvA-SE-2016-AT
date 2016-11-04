module Common::StringTemplate

import String;
import IO;
import Set;
import List;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */

// Capitalize first char of string
public str capitalize(str s) {
	return toUpperCase(substring(s, 0, 1)) + substring(s, 1);
}

// Helper to generate a setter
private str genSetter(map[str, str] fields, str x) {
	return "public void set<capitalize(x)>(<fields[x]> <x>) {
		   '	this.<x> = <x>;
		   '}";
}

// Gelper to generate a getter
private str genGetter(map[str, str] fields, str x) {
	return "public <fields[x]> get<capitalize(x)>() {
		   '	return <x>;
		   '}";
}

// Generate a class with given name and fields.
// Field names are processed in sorted order.
public str genClass(str name, map[str, str] fields) {
	return
		"public class <name> {
		'<for (x <- sort([f | f <- fields])) {> 
		'	private <fields[x]> <x>;<}>
		'<for (x <- sort([f | f <- fields])) {> 
		'	<genSetter(fields, x)>
		'
		' 	<genGetter(fields, x)>
		'<}>
		'}";
}

public map[str, str] theFields = (
	"name": "String", 
	"age": "Integer", 
	"address": "String"
	);

// > println(genClass("Person", theFields));
