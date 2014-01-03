Clusandra: A Data Stream Clustering Framework
=============================================

Clusandra is a data stream clustering framework that was started as part of a CS master's project at the 
University of West Florida. It is written entirely in the Java programming language and leverages the 
Spring framework and any JMS-compliant messaging system. 

The general idea behind CluSandra is to provide a framework that facilitates the distributed processing of the data stream. 

Included is an example distributed processing workflow or pipeline that leverages the CluSandra framework in order to control or harness the data stream. The pipeline clusters/groups the data stream elements in both a temporal and spatial manner.  In other words, it groups the elements of the data stream that are both temporally and spatially similar to one another. The resulting cluster, or microclusters, are persited to the Cassandra No SQL database. The pipeline is loosely modeled after the map-reduce design pattern and incorporates the concept of a cluster-feature tree described in the BIRCH paper. 

You can couple CluSandra with an integration framework, like Apacha Camel, to implement some interesting workflows or messaging design patterns. 

See the trunk/doc directory for papers produced during the project and referenced works . The papers need to be 
updated to reflect the latest work on Clusandra. 

As I get the time, I continue to evolve Clusandra. Most recently added KMeans-based algorithms to provide better
clusterings. Also incorporated KMeans++ for initial Kmeans seeding, triangle inequality to accelerate K-means 
(http://cseweb.ucsd.edu/~elkan/kmeansicml03.pdf) optimized version of euclidean distance formulas and 
data normalization using z-scores.  

Next step is to incorporate Cassandra's latest CQL. You'll note that Clusandra has a CQL, which is not to be confused 
with Cassandra's CQL. Clusandra-CQL is short for "Clustering Query Language". The initial Cassandra Java client used is 
Scale, but again, this should be replaced with Cassandra-CQL. I believe there is a JDBC type driver for Cassy CQL and 
if so, that should be incorporated. Maybe a Spring JDBC template?  
