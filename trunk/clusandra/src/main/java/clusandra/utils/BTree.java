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
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.ClusandraKernel;
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
 * @param <ClusandraKernel>
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
	private Node root;
	private int size;
	// used as index to all leaf nodes.
	private LinkedList<Node> leaves = new LinkedList<Node>();

	// used for synchronizing access to the tree
	private final ReentrantLock myLock = new ReentrantLock();

	// records last time of tree insertion
	private long lastClean = 0L;

	// the back ground thread that periodically cleans the treee
	private Thread runner;

	// three vars used for diagnostics/reporting purposes
	private int numNodes = 0;
	private int numLeaves = 0;
	private int numClusters = 0;

	/**
	 * TODO: Need to give BTree a CassandraDao so that it can persist
	 * microclusters out to Cassandra. Perhaps instead of CassandraDao we should
	 * be using a more generic Dao?
	 */

	/**
	 * Creates a new CFTree.
	 * 
	 * @param maxEntries
	 *            maximum number of entries per node
	 * @param numDims
	 *            the number of dimensions of the CFTree.
	 */
	public BTree(int maxEntries, int numDims) {
		this();
		this.maxEntries = maxEntries;
		this.numDims = numDims;
		root = buildRoot(true);
	}

	/**
	 * Builds a new CFTree using default settings. TODO: This constructor does
	 * not call buildRoot
	 */
	public BTree() {
		// runner = new Thread(this, "BTree thread: ");
		// runner.setDaemon(true);
		// runner.start();
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
	 * 
	 * @param maxEntries
	 */
	public void setMaxEntries(int maxEntries) {
		this.maxEntries = maxEntries;
	}

	/**
	 * @return the number of items in this tree.
	 */
	public int size() {
		return size;
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
	 * @param o
	 */
	public void setOverlapFactor(double o) {
		overlapFactor = o;
	}

	/**
	 * 
	 * @return the microcluster overlap factor.
	 */
	public double getOverlapFactor() {
		return overlapFactor;
	}

	/**
	 * Set the number of dimensions for this tree.
	 * 
	 * @param numDims
	 */
	public void setNumDims(int numDims) {
		this.numDims = numDims;
	}

	public int getNumDims() {
		return this.numDims;
	}

	/**
	 * Set the factor that determines if a microcluster is sparse or not; it is
	 * a percentage of its maximum density. For example, suppose the factor is
	 * set to 0.3, then if the microcluster's density falls below 30% of its
	 * maximum density, it is considered sparse.
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
			if (root.isEmpty()) {
				root = buildRoot(true);
			} else if (root.size() == 1 && !root.leaf) {
				root = root.children.get(0);
				root.parent = null;
			}
			return;
		}
		if (n.isEmpty()) {
			n.parent.removeChild(n);
		}
		condenseTree(n.parent);
	}

	/**
	 * Empties the CFTree
	 */
	public void clear() {
		try {
			lock();
			root = buildRoot(true);
			size = 0;
			leaves.clear();
		} finally {
			unlock();
		}
		// let the GC take care of the rest.
	}

	/**
	 * Inserts the given cluster into the CTree.
	 * 
	 * @param cluster
	 */
	public void insert(ClusandraKernel cluster) {

		LOG.debug("insert: entered with tree size = " + size);

		// first, find the closest leaf to this cluster; start from the root
		// node. The chooseLeaf method will update the center and N value of
		// the 'non-leaf' nodes.
		Node leaf = chooseLeaf(root, cluster);

		Entry entry = null;

		// now we need to determine which child (if any) of the chosen leaf node
		// has a cluster that is closest to the given cluster. A child of a leaf
		// is an 'Entry' that contains an actual microcluster, and a leaf may
		// contain many children.
		if (!leaf.isEmpty()) {

			LOG.debug("insert: leaf node has this many entries " + leaf.size());

			// first find the closest entry (cluster). restrict the search to
			// only those microclusters that are temporally relevant to the
			// given microcluster
			double minDist = Double.MAX_VALUE;
			for (Node child : leaf.getChildren()) {
				if (((Entry) child).isRelevant(cluster.getLST()
						/ cluster.getN())) {
					double dist = cluster.getDistance(child.center);
					if (dist < minDist) {
						entry = (Entry) child;
						minDist = dist;
					}
				}
			}

			// Do the two clusters spatially overlap?
			if (entry != null
					&& cluster
							.spatialOverlap(entry.cluster, getOverlapFactor())) {
				LOG.debug("insert: two microclusters spatially overlap");
				entry.absorb(cluster);
				// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
				// CASSANDRA
				return;
			}

			// determine if there is any entry in the leaf that is no
			// longer relevant wrt to this new cluster. if so, replace the
			// irrelevant entry with this new one. the irrelevant entry has
			// already been persisted out to cassandra
			for (Node child : leaf.getChildren()) {
				entry = (Entry) child;
				if (!entry.isRelevant(cluster.getLST() / cluster.getN())) {
					entry.remove();
					entry.parent.addChild(new Entry(cluster));
					// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
					// CASSANDRA
					return;
				}
			}

		} else {
			LOG.debug("insert: leaf node is empty");
		}

		// add the entry to the tree
		entry = new Entry(cluster);
		leaf.addChild(entry);
		size++;
		entry.parent = leaf;
		LOG.debug("insert: adding micro cluster, tree size = " + size);
		LOG.debug("insert: leaf size  = " + leaf.size());
		// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
		// CASSANDRA
		if (leaf.size() > maxEntries) {
			Node[] splits = splitNode(leaf);
			adjustTree(splits[0], splits[1]);
		} else {
			// why are we doing this?!
			adjustTree(leaf, null);
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
		numNodes = 0;
		numLeaves = 0;
		numClusters = 0;
		printStats(root);
		return new String("\nnumber of nodes in tree = " + numNodes + "\n"
				+ "number of leaves in tree = " + numLeaves + "\n"
				+ "number of leaves in list = " + leaves.size() + "\n"
				+ "number of clusters in tree = " + numClusters + "\n"
				+ "trees size = " + size + "\n");
	}

	/**
	 * Background thread that performs housekeeping chores TODO: prior to
	 * cleaning check how long it has been since last tree update
	 */
	public void run() {

		LOG.trace(runner.getName() + ": btree clean thread has started");

		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignore) {
			}

			if (size == 0) {
				LOG.trace("btree clean thread: tree is empty");
				continue;
			}

			// perform house cleaning if it has been 5 seconds or more since
			// last clean job
			if (System.currentTimeMillis() - lastClean >= 5000) {
				// don't wait on the lock
				if (!isLocked()) {
					try {
						lock();
						LOG.debug(runner.getName() + ": tree housecleaning");
						clean(root);
						lastClean = System.currentTimeMillis();
					} finally {
						unlock();
					}
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

	private void printStats(Node n) {
		// only leaves contain clusters
		if (n.leaf) {
			++numLeaves;
			numClusters += n.size();
		} else {
			++numNodes;
			for (Node node : n.children) {
				printStats(node);
			}
		}
	}

	/**
	 * Looks for tree entries that are no longer temporally relevant
	 * 
	 * @param node
	 */
	private void clean(Node node) {
		// only leaves contain clusters
		if (node.isLeaf()) {
			boolean removed;
			do {
				removed = false;
				for (Node e : node.getChildren()) {
					Entry entry = (Entry) e;
					if (!entry.isRelevant()) {
						entry.remove();
						if (node.isEmpty()) {
							condenseTree(node);
						}
						removed = true;
						break;
					}
				}
			} while (removed);
		} else {
			for (ListIterator<Node> li = node.getChildren().listIterator(); li
					.hasNext();) {
				clean(li.next());
			}
		}
	}

	/**
	 * Iterate through the children of the given node until a suitable leaf has
	 * been reached.
	 * 
	 * @param n
	 * @param e
	 * @return
	 */
	private Node chooseLeaf(Node n, ClusandraKernel c) {
		// return if n is a leaf
		if (n.leaf) {
			return n;
		}
		// else, continue working down through the tree until a suitable
		// leaf is found
		double minDistance = Double.MAX_VALUE;
		Node next = null;
		for (Node child : n.children) {
			double distance = c.getDistance(child.center);
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

	private void adjustTree(Node n, Node nn) {
		if (n == root) {
			if (nn != null) {
				// build new non-leaf root and add children.
				root = buildRoot(false);
				root.addChild(n);
				root.addChild(nn);
			}
			return;
		}

		if (nn != null) {
			if (n.parent.size() > maxEntries) {
				Node[] splits = splitNode(n.parent);
				adjustTree(splits[0], splits[1]);
			}
		}

		if (n.parent != null) {
			adjustTree(n.parent, null);
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

		LOG.debug("splitNode: entered");

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
		nn[0].children.clear();
		// now distribute the children across the two nodes
		for (int i = 0; i < children.size(); i++) {
			Node node = children.get(i);
			if ((i % 2) == 0) {
				nn[0].addChild(node);
			} else {
				nn[1].addChild(node);
			}
		}

		LOG.debug("splitNode: nn[0] size = " + nn[0].size());
		LOG.debug("splitNode: nn[1] size = " + nn[1].size());
		return nn;
	}

	/**
	 * There are two types of nodes in the tree: Node and Leaf.
	 * 
	 * A Node contains other Nodes, while a Leaf contains an Entry, which
	 * encapsulates a microcluster
	 * 
	 * @author jfernandez
	 * 
	 */
	private class Node {

		double N = 0.0d;
		double center[];
		LinkedList<Node> children = new LinkedList<Node>();
		boolean leaf;
		Node parent;

		private Node(int dimensions, boolean leaf) {
			this.leaf = leaf;
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
			N += child.N;
			for (int i = 0; i < center.length; i++) {
				center[i] = (center[i] + child.center[i]) / N;
			}
			children.add(child);
			child.parent = this;
		}

		void removeChild(Node child) {
			N -= child.N;
			for (int i = 0; i < center.length; i++) {
				center[i] = (center[i] - child.center[i]) / N;
			}
			children.remove(child);
		}

		void addCenter(double nN, double[] nC) {
			N += nN;
			for (int i = 0; i < center.length; i++) {
				center[i] = (center[i] + nC[i]) / N;
			}
		}

		void subCenter(double nN, double[] nC) {
			N -= nN;
			for (int i = 0; i < center.length; i++) {
				center[i] = (center[i] - nC[i]) / N;
			}
		}

		int size() {
			return children.size();
		}

		boolean isEmpty() {
			return children.isEmpty();
		}
	}

	/**
	 * An entry is only found in leaf nodes and contains a microcluster
	 * 
	 * @author jfernandez
	 * 
	 */
	private class Entry extends Node {
		ClusandraKernel cluster;
		// must start off with a max density
		double density = getMaximumDensity();

		Entry(ClusandraKernel cluster) {
			super(cluster.getCenter().length, true);
			this.cluster = cluster;
			// assign this node's center, the cluster's center
			center = Arrays.copyOf(cluster.getCenter(), center.length);
			N = cluster.getN();
		}

		ClusandraKernel getCluster() {
			return cluster;
		}

		double getDensity() {
			return density;
		}

		void absorb(ClusandraKernel target) {
			// before absorbing the given cluster, set the new density based on
			// the creation time of the given cluster
			// ???????????????????
			density = getTemporalDensity(target.getCT());
			cluster.merge(target);
			center = Arrays.copyOf(cluster.getCenter(), center.length);
			N = cluster.getN();
		}

		// remove this Entry from the tree. Entries are found only in leaf
		// nodes!
		void remove() {
			parent.getChildren().remove(this);
			Node nextParent = parent;
			while (nextParent != root) {
				nextParent.subCenter(this.N, this.center);
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
