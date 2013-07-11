clusandra
=========

Clusandra is a data stream framework that was started as part of a CS master's project at the University of West Florida. 

The general idea is to have a framework that allows for the distributed or parallel processing of the data stream.

See the trunk/doc directory for papers produced during the project and referenced works . The papers need to be 
updated to reflect the latest work on Clusandra. 

Clusandra also incorporates clustering algorithms used for harnessing the fast and evolving data stream. Much of this
is based on the concept of cluster-feature trees and the BIRCH paper. 

As I get the time, I continue to evolve Clusandra. Most recently added KMeans-based algorithms to provide better
clusterings. Also incorporated KMeans++ for initial Kmeans seeding.  

Next step is to incorporate Cassandra's latest CQL. You'll note that Clusandra has a CQL, which is not to be confused 
with Cassandra's CQL. Clusandra-CQL is short for "Clustering Query Language". The initial Cassandra Java client used is 
Scale, but again, this should be replaced with Cassandra-CQL. I believe there is a JDBC type driver for Cassy CQL and 
if so, that should be incorporated. Maybe a Spring JDBC template?  
