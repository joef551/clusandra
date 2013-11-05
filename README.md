Clusandra: A Data Stream Clustering Framework
=============================================

Clusandra is a data stream clustering framework that was started as part of a CS master's project at the 
University of West Florida. It is written entirely in the Java programming language and leverages the 
Spring framework and any JMS-compliant messaging system. 

The general idea is to have a framework that facilitates the distributed processing of the data stream. It also allows you
to pipeline the processing of the data stream and persist synopsis structures in the Cassandra NoSQL database. For 
example, you can pipeline a series of modules that preprocess, cluster, and then persist the clusters to the database. 
Using a integration framework, such as Apache Camel, you can implement a number of different pipelining designs. 

See the trunk/doc directory for papers produced during the project and referenced works . The papers need to be 
updated to reflect the latest work on Clusandra. 

Clusandra also incorporates clustering algorithms used for harnessing the fast and evolving data stream. Much of this
is based on the concept of cluster-feature trees and the BIRCH paper. 

As I get the time, I continue to evolve Clusandra. Most recently added KMeans-based algorithms to provide better
clusterings. Also incorporated KMeans++ for initial Kmeans seeding, triangle inequality to accelerate K-means 
(http://cseweb.ucsd.edu/~elkan/kmeansicml03.pdf) optimized version of euclidean distance formulas and 
data normalization using z-scores.  

Next step is to incorporate Cassandra's latest CQL. You'll note that Clusandra has a CQL, which is not to be confused 
with Cassandra's CQL. Clusandra-CQL is short for "Clustering Query Language". The initial Cassandra Java client used is 
Scale, but again, this should be replaced with Cassandra-CQL. I believe there is a JDBC type driver for Cassy CQL and 
if so, that should be incorporated. Maybe a Spring JDBC template?  
