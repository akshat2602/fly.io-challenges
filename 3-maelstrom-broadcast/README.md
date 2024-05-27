1st problem: type conversion due to maelstrom's way of sending body, not really a problem

2nd problem: deciding on whether to do some kind of graph traversal and send that data to the nodes or just do atleast once semantics and ignore retransmission if the node has already seen this particular value before

3rd problem: When and how to clear the channel to ensure no messages are lost. Not really a problem because channels are blocking

4th problem: concurrent map writes, need to add a 

5th problem: performance