Initial thoughts on how to implement: 

Add behaviour: 
For add, first perform a read, add the value and then try a compare and swap, if it returns false then store new value with a different key with the original read and process in the background

Read behaviour: 
Always read from kv

Background add processing: 
