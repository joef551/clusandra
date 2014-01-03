/*
 * COPYRIGHT(c) 2013 by Jose R. Fernandez
 *
 * This file is part of CluSandra.
 *
 * CluSandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CluSandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CluSandra.  If not, see <http://www.gnu.org/licenses/>.
 *
 * $Date:  $
 * $Revision: $
 * $Author: $
 * $Id: $
 */
package clusandra.utils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.MicroCluster;
import static clusandra.utils.DateUtils.getClusandraDate;

/**
 * Implementation of a BTree for objects that represent microclusters or cluster
 * features (CFs), thus also referred to as a CFTree. The tree and its nodes
 * evolve as microclusters are inserted into the tree. For example, a
 * microcluster that is being inserted may be absorbed by its closest
 * microcluster in the tree. Each non-leaf node in the tree represents a cluster
 * (subtree) and each leaf node is an actual microcluster. microclusters absorb
 * other microclusters and are removed from the tree when they become sparse.
 * 
 * Absorption is controlled by the overlap factor. For example, if the factor is
 * set to 1.0, then a microcluster, which is being inserted, will be absorbed by
 * its nearest microcluster iff the two microcluster's radii overlap. If the
 * factor is set to 0.5, then they'll overlap iff 0.5 of their radii overlap. So
 * in the latter case, the microclusters must be closer to one another than in
 * the former case.
 * 
 * Any time a microcluster is updated, it will be written out to the DB. It is
 * removed from the tree when it becomes sparse. Sparseness or its temporal
 * density is controlled by a temporal decay factor (lambda).
 * 
 * The density portion of this work is based, in part, on the following paper.
 * 
 * Citation: Yixin Chen, Li Tu: Density-Based Clustering for Real-Time Stream
 * Data. KDD '07
 * 
 * The above paper describes an approach for managing the temporal density of
 * microclusters without having to visit a microcluster each and every time
 * period; as described in:
 * 
 * Citation: Feng Cao, Martin Ester, Weining Qian, Aoying Zhou: Density-Based
 * Clustering over an Evolving Data Stream with Noise. SDM 2006
 * 
 * 
 * This class is not thread-safe.
 * 
 * @param <MicroCluster>
 *            the type of entry to store in this BTree.
 */
public class BTree implements Runnable {

	private static final Log LOG = LogFactory.getLog(BTree.class);

	public static final int MAX_ENTRIES = 5;

	// the maximum number of entries (children) that can occupy a tree node.
	private int maxEntries = MAX_ENTRIES;
	private int numDims = 2;
	// The overlap factor controls the merging of microclusters. If the factor
	// is set to 1.0, then the two microclusters will merge iff their radii
	// overlap. If the factor is set to 0.5, then the two will merge iff
	// one-half their radii overlap. So in the latter case, the micr-clusters
	// must be much closer to one another.
	private double overlapFactor = 1.0d;
	// Lambda is the forgetfulness factor. It dictates how quickly a cluster
	// becomes temporally irrelevant. The lower the value for lambda, the quick
	// a microcluster will become irrelevant.
	private double lambda = 0.5d;
	// the density, as a factor of maximum density, that a cluster is considered
	// irrelevant. So if the factor is set to 0.25, then the microcluster will
	// become temporally irrelevant if its density falls below 25% of its
	// maximum density.
	private double sparseFactor = 0.25d;

	// the tree's root Node
	private Node root;

	// used as index to all leaf nodes.
	private LinkedList<Node> leaves = new LinkedList<Node>();

	// used for synchronizing access to the tree
	private final ReentrantLock myLock = new ReentrantLock();

	// the background thread that does the house cleaning
	private Thread runner;

	// the interval at which the tree may be updated by the tree owner
	private long updateInterval;

	// used for counting the number of non-Leaf Nodes in the tree
	private int numNodes = 0;

	// used for keeping track of when the tree was last updated.
	private long lastModificationTime = 0L;

	/**
	 * TODO: Need to give BTree a CassandraDao so that it can persist
	 * microclusters out to Cassandra. Perhaps instead of CassandraDao we should
	 * be using a more generic Dao?
	 */

	/**
	 * Creates a new BTree.
	 * 
	 * @param maxEntries
	 *            maximum number of entries per node
	 * @param numDims
	 *            the number of dimensions of the CFTree.
	 */
	public BTree(int maxEntries, int numDims) {
		this.maxEntries = maxEntries;
		this.numDims = numDims;
		root = buildRoot(true);
	}

	/**
	 * Use this contructor to create a BTree with a background housekeeping
	 * thread.
	 * 
	 * @param maxEntries
	 * @param numDims
	 * @param updateInterval
	 */
	public BTree(int maxEntries, int numDims, long updateInterval) {
		this(maxEntries, numDims);
		this.updateInterval = updateInterval;
		startThread();
	}

	/**
	 * Starts the housecleaning thread.
	 */
	private void startThread() {
		runner = new Thread(this, "BTree thread: ");
		runner.setDaemon(true);
		runner.start();
	}

	/**
	 * 
	 * @return the root node of the tree
	 */
	public Node getRoot() {
		return root;
	}

	/**
	 * @return the maximum number of entries per node
	 */
	public int getMaxEntries() {
		return maxEntries;
	}

	/**
	 * Set max number of entries per node.
	 * 
	 * @param maxEntries
	 */
	public void setMaxEntries(int maxEntries) {
		this.maxEntries = maxEntries;
	}

	/**
	 * Set the temporal decay factor.
	 * 
	 * @param lambda
	 *            a value that must be greater than 0.0 and less than 1.0
	 */
	public void setLambda(double lambda) {
		this.lambda = lambda;
	}

	/**
	 * 
	 * @return the temporal decay factor
	 */
	public double getLambda() {
		return lambda;
	}

	/**
	 * Set the microcluster overlap factor.
	 * 
	 * @param overlapFactor
	 */
	public void setOverlapFactor(double overlapFactor) {
		this.overlapFactor = overlapFactor;
	}

	/**
	 * 
	 * @return the microcluster overlap factor.
	 */
	public double getOverlapFactor() {
		return overlapFactor;
	}

	/**
	 * Returns the number of dimensions for the tree.
	 * 
	 * @return
	 */
	public int getNumDims() {
		return this.numDims;
	}

	/**
	 * Get the tree's last modification time.
	 * 
	 * @return
	 */
	public long getLastModificationTime() {
		return lastModificationTime;
	}

	/**
	 * Set the tree's last modification time.
	 * 
	 * @param lastModificationTime
	 */
	public void setLastModificationTime(long lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
	}

	/**
	 * Set the factor that determines if a microcluster is temporally sparse or
	 * not; it is a percentage of its maximum density. For example, suppose the
	 * factor is set to 0.3, then if the microcluster's density falls below 30%
	 * of its maximum density, it is considered sparse.
	 * 
	 * @param sf
	 *            a value that must be greater than 0.0 and less than 1.0
	 */
	public void setSparseFactor(double sf) {
		if (sf > 0.0d && sf < 1.0d) {
			sparseFactor = sf;
		}
	}

	public double getSparseFactor() {
		return sparseFactor;
	}

	/**
	 * Build the root node
	 * 
	 * @param asLeaf
	 *            specifies whether root is a leaf or not.
	 * @return
	 */
	private Node buildRoot(boolean asLeaf) {
		return new Node(numDims, asLeaf);
	}

	/**
	 * 
	 * @return The maximum density, which is 1/(1-lambda).
	 */
	private double getMaximumDensity() {
		return (1.0d / (1.0d - getLambda()));
	}

	/**
	 * From the given node, work up the tree and remove all empty nodes
	 * 
	 * @param n
	 */
	private void condenseTree(Node n) {
		if (n == root) {
			// we're at the top of the tree
			if (root.isEmpty()) {
				if (!root.isLeaf()) {
					leaves.add(root);
					root.leaf = true;
				}
				// make sure the root is reset if empty
				root.clearCenter();
			} else if (root.size() == 1 && !root.leaf) {
				root = root.children.getFirst();
				root.parent = null;
			}
			return;
		}
		// if the node is not empty, then there is no need to proceed any
		// further
		else if (!n.isEmpty()) {
			return;
		}
		// the node is empty, so remove it from its parent
		n.parent.removeChild(n);
		// if this was a leaf, then remove it from the leaves index
		if (n.isLeaf()) {
			leaves.remove(n);
		}
		// check the parent
		condenseTree(n.parent);
	}

	/**
	 * Empties the BTree
	 */
	public void clear() {
		lock();
		try {
			root = buildRoot(true);
			leaves.clear();
		} finally {
			unlock();
		}
		// let the GC take care of the rest.
	}

	/**
	 * Inserts the given cluster into the BTree.
	 * 
	 * @param cluster
	 */
	public void insert(MicroCluster cluster) {

		LOG.debug("insert: entered with tree size = " + getNumClusters());
		LOG.debug("insert: given cluster's size = " + cluster.getN());

		// update the tree's modification time.
		touch();

		// find the closest leaf to the cluster being inserted; start from the
		// root
		// node. The chooseLeaf method will update the center and N values of
		// the 'non-leaf' (i.e., intermediate) nodes.
		Node leaf = chooseLeaf(root, cluster);

		Entry entry = null;
		Entry ientry = null;

		// now determine which child (if any) of the chosen leaf node
		// has a cluster that is closest to the given cluster. A child of a leaf
		// is an 'Entry' that contains an actual microcluster, and a leaf may
		// contain many children.
		if (!leaf.isEmpty()) {

			LOG.debug("insert: leaf node has this many micro-clusters "
					+ leaf.size());

			// of the microclusters in the Leaf node, find the one closest to
			// the cluster being inserted. restrict the search to only those
			// microclusters that are temporally relevant to the given
			// microcluster. mark each entry in the leaf as being relevant or
			// not; this'll be used later
			double minDist = Double.MAX_VALUE;
			for (Node child : leaf.getChildren()) {
				Entry childEntry = (Entry) child;
				if (childEntry.isRelevant(cluster.getLST() / cluster.getN())) {
					double dist = cluster.getDistance(child.center);
					if (dist < minDist) {
						entry = childEntry;
						minDist = dist;
					}
				}
				// save any irrelevant entry
				else if (ientry == null) {
					ientry = childEntry;
				}
			}

			if (entry != null) {

				// note: if a cluster has only one point, then its either an
				// outlier
				// or an orphan

				// if both the cluster being inserted and the one found for it
				// in the tree are both outliers or orphans, then simply
				// insert the cluster
				if (entry.cluster.getN() == 1 && cluster.getN() == 1) {
					LOG.debug("insert: both cluster being inserted and one "
							+ "found for it are outliers/orphans");
				}

				// if the cluster being inserted is an outlier or an orphan,
				// then see if it can fit in with the cluster of points
				// that is closest to it, i.e., see if the orphan lies within 2
				// standard deviations of the cluster in the tree
				else if (cluster.getN() == 1) {
					// if it is within 2 standard deviations, then consider it
					// an orphan
					double radius = entry.cluster.getRadius() * 2;
					LOG.debug("insert: procesing outlier or orphan, "
							+ "minDist and radius = " + minDist + ", " + radius);
					if (minDist <= radius) {
						LOG.debug("insert: found home for orphan");
						entry.absorb(cluster);
						// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
						// CASSANDRA
						return;
					}
				}

				// the cluster being inserted has more than one point, so it is
				// not an orphan or outlier; however, the one found for it in
				// the tree is an outlier or orphan. so just do the opposite of
				// above
				else if (entry.cluster.getN() == 1) {
					double radius = cluster.getRadius() * 2;
					LOG.debug("insert: procesing outlier or orphan, "
							+ "minDist and radius = " + minDist + ", " + radius);
					if (minDist <= radius) {
						LOG.debug("insert: found home for orphan");
						entry.absorb(cluster);
						// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
						// CASSANDRA
						return;
					}
				}

				// finally, neither of the two is an outlier or an orphan, so
				// see the two spatially overlap
				else if (cluster.spatialOverlap(entry.cluster,
						getOverlapFactor())) {
					LOG.debug("insert: two microclusters spatially overlap");
					entry.absorb(cluster);
					// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
					// CASSANDRA
					return;
				}
			}

			// if we get to here, the cluster being inserted has not found a
			// home with an existing cluster in the tree

			// if there is any entry in the leaf that is no longer relevant wrt
			// to this new cluster, replace the irrelevant entry with this new
			// one. the irrelevant entry has already been persisted out to
			// cassandra
			if (ientry != null) {
				LOG.debug("insert: replacing irrelevant entry");
				ientry.remove();
				ientry.parent.addChild(new Entry(cluster));
				// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
				// CASSANDRA
				return;
			}

		} else {
			LOG.debug("insert: chosen leaf node is empty");
		}

		// add an Entry to the Leaf node; an Entry node is hosted only by Leaf
		// nodes and encapsulates a MicroCluster
		entry = new Entry(cluster);
		leaf.addChild(entry);
		LOG.debug("insert: adding micro cluster");
		// LOG.debug("insert: leaf size  = " + leaf.size());

		// YOU NOW HAVE TO WRITE THE NEWLY ENTERED ENTRY'S CLUSTER OUT TO
		// CASSANDRA

		// if the leaf has gotten too big, then split it
		if (leaf.size() > getMaxEntries()) {
			Node[] splits = splitNode(leaf);
			adjustTree(splits[0], splits[1]);
		}
	}

	/**
	 * Iterate through the entire tree and return some stats on the tree as a
	 * string.
	 * 
	 * @param n
	 * @param e
	 * @return
	 */
	public String printStats() {
		return new String("\nnumber of nodes in tree = " + countNodes() + "\n"
				+ "number of leaves in list = " + leaves.size() + "\n"
				+ "number of clusters in tree = " + getNumClusters() + "\n");
	}

	/**
	 * Background thread that performs housekeeping chores TODO: prior to
	 * cleaning check how long it has been since last tree update
	 */
	public void run() {

		// we check to see if the tree has been updated within two update
		// interval times. one interval is the time amount of time the queue
		// manager waits for messages to arrive. so, if nothing has arrived
		// within two of these intervals, then it is safe to check the tree.
		long interval = getUpdateInterval() * 2;
		long currentTime = 0L;
		long lastModTime = 0L;

		LOG.debug(runner.getName() + ": started with this interval: "
				+ interval);

		while (true) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException ignore) {
			}
			currentTime = System.currentTimeMillis();
			lastModTime = getLastModificationTime();
			// perform house cleaning if the tree has not been updated within
			// the minimum interval time
			if (currentTime - lastModTime >= interval) {
				if (!isLocked()) {
					lock();
					try {
						if (getLastModificationTime() == lastModTime) {
							if (clean()) {
								LOG.debug(runner.getName()
										+ ": tree was condensed");
								LOG.debug(printStats());
							}
						} else {
							LOG.trace(runner.getName()
									+ ": tree updated since lock acquired");
						}
					} finally {
						unlock();
					}
				} else {
					LOG.trace(runner.getName() + ": lock was taken");
				}
			}
		}
	}

	/*
	 * This series of methods are used to control access to this BTree
	 */
	public void lock() {
		myLock.lock();
	}

	public void unlock() {
		myLock.unlock();
	}

	public boolean isLocked() {
		return myLock.isLocked();
	}

	public boolean tryLock() {
		return myLock.tryLock();
	}

	public boolean isWaiting() {
		return myLock.hasQueuedThreads();
	}

	public long getUpdateInterval() {
		return updateInterval;
	}

	public void setUpdateInterval(long updateInterval) {
		this.updateInterval = updateInterval;
	}

	/**
	 * This method is used to set the last modification time of the tree to the
	 * current time.
	 */
	private void touch() {
		setLastModificationTime(System.currentTimeMillis());
	}

	/**
	 * Starting from the top of the tree, count the number of Nodes (not Leaves)
	 * in the tree.
	 * 
	 * @return
	 */
	private int countNodes() {
		numNodes = 0;
		countNodes(root);
		return numNodes;
	}

	private void countNodes(Node n) {
		if (!n.isLeaf()) {
			++numNodes;
			for (Node node : n.getChildren()) {
				countNodes(node);
			}
		}
	}

	/**
	 * Starting from the leaves, looks for leaf entries that are no longer
	 * temporally relevant and removes them from the tree
	 */
	private boolean clean() {
		boolean cleaned = false;
		List<Node> myLeaves = new ArrayList<Node>(leaves);

		// check and see if the tree is void of clusters
		if (getNumClusters() == 0) {
			LOG.trace("clean: tree has no clusters, its empty");
			return cleaned;
		}

		LOG.trace("clean: checking for stale clusters");
		// iterate through all the leaves
		for (Node leaf : myLeaves) {
			// iterate through all the entries in this leaf
			List<Node> entries = new ArrayList<Node>(leaf.getChildren());
			for (Node node : entries) {
				Entry entry = (Entry) node;
				if (!entry.isRelevant()) {
					entry.remove();
				}
			}
			// if the leaf is empty, remove it from the tree
			if (leaf.isEmpty()) {
				cleaned = true;
				condenseTree(leaf);
			}
		}
		return cleaned;
	}

	/**
	 * Iterate through the children of the given node until a suitable leaf has
	 * been reached.
	 * 
	 * @param n
	 * @param e
	 * @return
	 */
	private Node chooseLeaf(Node n, MicroCluster c) {
		// return if n is a leaf
		if (n.leaf) {
			return n;
		}
		// else, continue working down through the tree until a suitable
		// leaf is found
		double minDistance = Double.MAX_VALUE;
		Node next = null;
		for (Node child : n.children) {
			double distance = MicroCluster.getDistance(c.getCenter(),
					child.center);
			if (distance < minDistance) {
				minDistance = distance;
				next = child;
			}
		}
		// if this is a non-leaf node, update its center of gravity. we do
		// this because the given cluster will be added to this node's subtree
		if (!next.leaf) {
			next.addCenter(c.getN(), c.getCenter());
		}
		return chooseLeaf(next, c);
	}

	/**
	 * Called immediately after a leaf has been split.
	 * 
	 * @param n
	 * @param nn
	 */
	private void adjustTree(Node n, Node nn) {
		LOG.trace("adjustTree: entered, root = " + (n == root));
		if (n == root) {
			if (nn != null) {
				// build new non-leaf root and add children.
				LOG.trace("adjustTree: building new root");
				root = buildRoot(false);
				root.addChild(n);
				root.addChild(nn);
			}
			return;
		}
		if (n.parent.size() > maxEntries) {
			Node[] splits = splitNode(n.parent);
			adjustTree(splits[0], splits[1]);
		}
	}

	/**
	 * Split the given node because it has reached its max capacity. The
	 * 'center' and 'N' values of the node's parents, grandparents, etc. remain
	 * the same, so there is no need to update them.
	 * 
	 * @param n
	 *            the nodes to split
	 * @return two nodes; the children of the given node are distributed across
	 *         the two resulting ndoes.
	 */
	private Node[] splitNode(Node n) {

		LOG.trace("splitNode: entered");

		// create an array of two nodes, with the first element being
		// the node to split and the second a new entry. note that
		// the node being split can either be a node or leaf-node
		Node[] nn = new Node[] { n, new Node(n.center.length, n.leaf) };

		// the new node's parent is the same as the one being split
		nn[1].parent = nn[0].parent;

		// the parent must also adopt the new node. the parent's center
		// and N remain the same. All we're doing is splitting one of its
		// child nodes.
		if (nn[1].parent != null) {
			nn[1].parent.addChild(nn[1]);
		}

		// temporarily adopt the children of the node being split
		LinkedList<Node> children = new LinkedList<Node>(nn[0].children);

		// reset the node being split up
		nn[0].children.clear();
		nn[0].clearCenter();

		// distribute the children across the two nodes, making sure
		// those closest to one another end up in the same node

		// remove the first child, and add it to the first node
		Node lastNode = children.removeFirst();
		nn[0].addChild(lastNode);

		// remove the child that is furthest away from the one just
		// removed
		double maxDist = Double.NEGATIVE_INFINITY;
		Node maxNode = null;
		for (Node child : children) {
			double dist = MicroCluster.getDistance(lastNode.center,
					child.center);
			if (dist > maxDist) {
				maxDist = dist;
				maxNode = child;
			}
		}
		// add the furthest one to the second node
		nn[1].addChild(maxNode);
		children.remove(maxNode);

		// remove each remaining child and add it to the node to which it is
		// closest
		while (!children.isEmpty()) {
			lastNode = children.removeFirst();
			double d0 = getNearestDistance(lastNode, nn[0].children);
			double d1 = getNearestDistance(lastNode, nn[1].children);
			if (d0 <= d1) {
				nn[0].addChild(lastNode);
			} else {
				nn[1].addChild(lastNode);
			}
		}

		LOG.trace("splitNode: nn[0] size = " + nn[0].size());
		LOG.trace("splitNode: nn[1] size = " + nn[1].size());
		return nn;
	}

	/**
	 * Returns the number of clusters in the tree
	 * 
	 * @return
	 */
	private int getNumClusters() {
		int numClusters = 0;
		if (!leaves.isEmpty()) {
			for (Node leaf : leaves) {
				numClusters += leaf.size();
			}
		}
		return numClusters;
	}

	/**
	 * Get the distance of the nearest Node in the list to that of the given
	 * Node
	 * 
	 * @param node
	 * @param nodes
	 * @return
	 */
	private static double getNearestDistance(Node node, LinkedList<Node> nodes) {
		double minDistance = Double.MAX_VALUE;
		for (Node tNode : nodes) {
			double distance = MicroCluster.getDistance(node.center,
					tNode.center);
			minDistance = (distance < minDistance) ? distance : minDistance;
		}
		return minDistance;
	}

	/**
	 * There are two types of nodes in the tree: Node and Leaf.
	 * 
	 * A Node contains other Nodes, while a Leaf contains one or more Entry
	 * nodes, which encapsulates a microcluster
	 * 
	 * @author jfernandez
	 * 
	 */
	private class Node {

		double N = 0.0d;
		double center[];
		double LS[];
		LinkedList<Node> children = new LinkedList<Node>();
		boolean leaf;
		Node parent;

		private Node(int dimensions, boolean leaf) {
			this.leaf = leaf;
			LS = new double[dimensions];
			center = new double[dimensions];
			// if this is a leaf, add it to the leaf index
			if (leaf) {
				leaves.add(this);
			}
		}

		boolean isLeaf() {
			return leaf;
		}

		LinkedList<Node> getChildren() {
			return children;
		}

		void addChild(Node child) {
			addCenter(child.N, child.LS);
			children.add(child);
			child.parent = this;
		}

		void removeChild(Node child) {
			subCenter(child.N, child.LS);
			children.remove(child);
		}

		void addCenter(double nN, double[] nC) {
			N += nN;
			for (int i = 0; i < LS.length; i++) {
				LS[i] += nC[i];
				center[i] = LS[i] / N;
			}
		}

		void subCenter(double nN, double[] nC) {
			N -= nN;
			for (int i = 0; i < center.length; i++) {
				LS[i] -= nC[i];
				center[i] = LS[i] / N;
			}
		}

		void clearCenter() {
			N = 0.0d;
			Arrays.fill(center, 0.0d);
			Arrays.fill(LS, 0.0d);
		}

		int size() {
			return children.size();
		}

		boolean isEmpty() {
			return children.isEmpty();
		}
	}

	/**
	 * An Entry node contains a microcluster and it is only found in a Leaf
	 * node.
	 * 
	 * @author jfernandez
	 * 
	 */
	private class Entry extends Node {
		MicroCluster cluster;
		// must start off with a max density
		double density = getMaximumDensity();

		Entry(MicroCluster cluster) {
			super(cluster.getLS().length, false);
			this.cluster = cluster;
			addCenter(cluster.getN(), cluster.getLS());
		}

		MicroCluster getCluster() {
			return cluster;
		}

		double getDensity() {
			return density;
		}

		/*
		 * Remember that chooseLeaf will have added the given target cluster to
		 * all the parent nodes!
		 */
		void absorb(MicroCluster target) {
			// before absorbing the given cluster, set the new density based on
			// the creation time of the given cluster
			// ??
			density = getTemporalDensity(target.getCT());
			cluster.merge(target);
			clearCenter();
			addCenter(cluster.getN(), cluster.getLS());
		}

		// Remove this Entry from the tree.
		void remove() {
			parent.removeChild(this);
			Node nextParent = parent.parent;
			while (nextParent != null) {
				nextParent.subCenter(N, LS);
				nextParent = nextParent.parent;
			}
		}

		boolean isRelevant(double time) {
			LOG.trace("isRelevant: entered with this time = "
					+ getClusandraDate((long) time));
			double td = getTemporalDensity(time);
			LOG.trace("isRelevant: calculated density  = " + td);
			double ratio = (td - 1.0d) / (getMaximumDensity() - 1.0d);
			LOG.trace("isRelevant: ratio  = " + ratio);
			return ratio >= getSparseFactor();
		}

		boolean isRelevant() {
			return isRelevant(System.currentTimeMillis());
		}

		/**
		 * Based on the cluster average timestamp (ms since epoc), return the
		 * temporal density of this cluster relative to the given time.
		 * 
		 * @param entry
		 * @param time
		 *            If time is set to 0, then the current time is used.
		 * @return
		 */
		private double getTemporalDensity(double time) {
			if (time == 0.0d) {
				time = System.currentTimeMillis();
			} else if (time <= getCluster().getLAT()) {
				return getDensity();
			}
			// transform the times from milliseconds to seconds
			time = time / 1000.0d;
			double avgT = (getCluster().getLST() / getCluster().getN()) / 1000.0d;

			LOG.trace("getTemporalDensity: delta = " + (time - avgT));

			double d1 = (Math.pow(getLambda(), Math.round(time - avgT)) * getDensity()) + 1.0d;

			LOG.trace("getTemporalDensity: d1 = " + d1);

			return (d1 > getMaximumDensity()) ? getMaximumDensity() : d1;
		}

	}
}
