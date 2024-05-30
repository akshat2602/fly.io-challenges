1st problem: type conversion due to maelstrom's way of sending body, not really a problem

2nd problem: deciding on whether to do some kind of graph traversal and send that data to the nodes or just do atleast once semantics and ignore retransmission if the node has already seen this particular value before

3rd problem: When and how to clear the channel to ensure no messages are lost. Not really a problem because channels are blocking

4th problem: concurrent map writes, need to add a lock

5th problem: performance

6th problem: Pushing to channel before replying to an RPC was causing issues with timeout XD

3e: 
Need to construct your own topology here because maximum latency can only be 600 ms and each exchange takes 100ms, maximum distance between two nodes by performing DFS comes out to be 8 so a custom topology is needed.