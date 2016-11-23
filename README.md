# sdcn-repository
Repository for SDCN projects

Module: jgroups-examples
SimpleChat

Module: dht
Simplified version of Pastry DHT, with the following assumptions:
- l=2.
- routing is perform using leaf set instead of routing table.
- operations between nodes: Join, UpdateLeafs, ...

Phases:
- 1st phase. Use of Java objects to implement nodes, messages between nodes are method invocations (i.e. no network,
  local environment). This phase is ised to create the ring with all the nodes.
