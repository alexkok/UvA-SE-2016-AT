module basic::Bubble

import List;
import IO;

// bubble sort 1

public list[int] sort1(list[int] numbers){
  if(size(numbers) > 0){
     for(int i <- [0 .. size(numbers)-1]){
       if(numbers[i] > numbers[i+1]){
         <numbers[i], numbers[i+1]> = <numbers[i+1], numbers[i]>;
         return sort1(numbers);
       }
    }
  }  
  
  return numbers;
}

bool isSorted(list[int] lst) = !any(int i <- index(lst), int j <- index(lst), (i < j) && (lst[i] > lst[j]));

test bool sorted1a() = isSorted([]);
test bool sorted1b() = isSorted([10]);
test bool sorted1c() = isSorted([10, 20]);
test bool sorted1d() = isSorted([-10, 20, 30]);
test bool sorted1e() = !isSorted([10, 20, -30]);

