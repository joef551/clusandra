clusandra
=========

Clusandra is a data stream framework that was started as part of a CS master's project at the University of West Florida. 

See the trunks/doc directory for papers produced during the project. The papers need to be updated to reflect the 
latest work on Clusandra. 

Clusandra also incorporates clustering algorithms used for harnessing the fast and evolving data stream. 

As I get the time, I continue to evolve Clusandra. Most recently added KMeans based algorithms to provide better
clusterings. 

Next step is to incorporate Cassandra's latest CQL. You'll note that Clusandra has a CQL, which is not to be confused 
with Cassandra's CQL. Clusandra-CQL is short for "Clustering Query Language". The initial Cassandra Java client used 
Scale, but again, this should be replaced with Cassandra-CQL. 
