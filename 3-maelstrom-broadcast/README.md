1st problem: type conversion due to maelstrom's way of sending body

2nd problem: deciding on whether to do some kind of graph traversal and send that data to the nodes or just do atleast once semantics and ignore retransmission if the node has already seen this particular value before

3rd problem: When and how to clear the channel to ensure no messages are lost