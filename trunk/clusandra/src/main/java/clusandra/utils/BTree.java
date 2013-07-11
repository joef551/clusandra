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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.BTreeClusterer;
import clusandra.clusterers.ClusandraKernel;

/**
 * Implementation of a BTree for objects that represent micro-clusters or
 * cluster features (CFs), thus also referred to as a CFTree. The tree and its
 * nodes evolve as micro-clusters are inserted into the tree. For example, a
 * micro-cluster that is being inserted may be absorbed by its closest
 * micro-cluster in the tree. Each non-leaf node in the tree represents a
 * cluster (subtree) and each leaf node is an actual micro-cluster.
 * Micro-clusters absorb other micro-clusters and are removed from the tree when
 * they become sparse.
 * 
 * Absorption is controlled by the overlap factor. For example, if the factor is
 * set to 1.0, then a micro-cluster, which is being inserted, will be absorbed
 * by its nearest micro-cluster iff the two micro-cluster's radii overlap. If
 * the factor is set to 0.5, then they'll overlap iff 0.5 of their radii
 * overlap. So in the latter case, the micro-clusters must be closer to one
 * another than in the former case.
 * 
 * Any time a micro-cluster is updated, it will be written out to the DB. It is
 * removed from the tree when it becomes sparse. Sparseness or its temporal
 * density is controlled by a temporal decay factor (lambda).
 * 
 * The density portion of this work is based, in part, on the following paper.
 * 
 * Citation: Yixin Chen, Li Tu: Density-Based Clustering for Real-Time Stream
 * Data. KDD '07
 * 
 * The above paper describes an approach for managing the temporal density of
 * micro-clusters without having to visit a micro-cluster each and every time
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
public class BTree {

	private static final Log LOG = LogFactory.getLog(BTree.class);

	public static final int MAX_ENTRIES = 5;

	// the maximum number of entries (children) that can occupy a tree node.
	private int maxEntries = MAX_ENTRIES;
	private int numDims = 2;
	// The overlap factor controls the merging of micro-clusters. If the factor
	// is set to 1.0, then the two micro-clusters will merge iff their radii
	// overlap. If the factor is set to 0.5, then the two will merge iff
	// one-half their radii overlap. So in the latter case, the micr-clusters
	// must be much closer to one another.
	private double overlapFactor = 1.0d;
	// Lambda is the forgetfulness factor. It dictates how quickly a cluster
	// becomes temporally irrelevant. The lower the value for lambda, the quick
	// a micro-cluster will become irrelevant.
	private double lambda = 0.5d;
	// the density, as a factor of maximum density, that a cluster is considered
	// irrelevant. So if the factor is set to 0.25, then the micro-cluster will
	// become temporally irrelevant if its density falls below 25% of its
	// maximum density.
	private double sparseFactor = 0.25d;
	private Node root;
	private int size;
	// used as index to all leaf nodes.
	private LinkedList<Node> leaves = new LinkedList<Node>();

	// three vars used for reporting purposes
	private int numNodes = 0;
	private int numLeaves = 0;
	private int numClusters = 0;

	/**
	 * Creates a new CFTree.
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
	 * Builds a new CFTree using default settings. TODO: This constructor does
	 * not call buildRoot
	 */
	public BTree() {
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
	 * Set the micro-cluster overlap factor.
	 * 
	 * @param o
	 */
	public void setOverlapFactor(double o) {
		overlapFactor = o;
	}

	/**
	 * 
	 * @return the micro-cluster overlap factor.
	 */
	public double getOverlapFactor() {
		return overlapFactor;
	}

	/**
	 * Set the factor that determines if a micro-cluster is sparse or not; it is
	 * a percentage of its maximum density. For example, suppose the factor is
	 * set to 0.3, then if the micro-cluster's density falls below 30% of its
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
		root = buildRoot(true);
		leaves.clear();
		// let the GC take care of the rest.
	}

	/**
	 * Cleans the tree of micro-clusters that have become temporally irrelevant.
	 */
	public void clean() {
		// the children of a leaf are of type Entry!
		// iterate through all the leaves and check their children. note
		// the use of a 'ListIterator' that allows us to remove on the fly
		for (ListIterator<Node> li = leaves.listIterator(); li.hasNext();) {
			Node leaf = li.next();
			for (Node node : leaf.children) {
				Entry entry = (Entry) node;
				// if the entry (cluster) is no longer relevant,
				// then remove it from its leaf parent
				if (!entry.isRelevant()) {
					entry.remove();
				}
			}
			// if the leaf is now empty (i.e., has no children) then condense
			// the tree and remove it from the leaves index
			if (leaf.isEmpty()) {
				condenseTree(leaf);
				li.remove();
			}
		}
	}

	/**
	 * Inserts the given cluster into the CTree.
	 * 
	 * @param cluster
	 */
	public void insert(ClusandraKernel cluster) {

		LOG.debug("insert: entered");

		// first, find the closest leaf to this cluster; start from the root
		// node. The chooseLeaf method will update the center and N value of
		// the 'non-leaf' nodes.
		Node leaf = chooseLeaf(root, cluster);

		// now we need to determine which child (if any) of the chosen leaf node
		// has a cluster that is closest to the given cluster. A child of a leaf
		// is an 'Entry', and a leaf may contain many children
		if (!leaf.isEmpty()) {

			LOG.debug("insert: leaf node has this many entries " + leaf.size());

			double minDist = Double.MAX_VALUE;
			Entry entry = null;
			for (Node child : leaf.children) {
				double dist = cluster.getDistance(child.center);
				if (dist < minDist) {
					entry = (Entry) child;
					minDist = dist;
				}
			}

			// Do the two clusters spatially overlap?
			if (cluster.spatialOverlap(entry.cluster, getOverlapFactor())) {
				// Do they also temporally overlap or is the entry's cluster
				// temporally relevant with respect to the given cluster?
				if (cluster.temporalOverlap(entry.cluster)
						|| !entry.isRelevant(cluster.getCT())) {
					// merge the two
					entry.absorb(cluster);
					LOG.debug("insert: two micro clusters overlap");
					// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
					// CASSANDRA
					return;
				}
			}

			// there is no spatial or temporal overlap. is the closest entry
			// relevant with respect to the current time?
			if (!entry.isRelevant()) {
				LOG.debug("insert: replacing irrelevant micro cluster");
				// remove the entry from the tree and replace it with the new
				// entry. Note that the entry being removed has already been
				// persisted to the DB.
				entry.remove();
				entry.parent.addChild(new Entry(cluster));
				// YOU NOW HAVE TO WRITE THE ENTRY'S CLUSTER OUT TO
				// CASSANDRA
				return;
			}

		} else {
			LOG.debug("insert: leaf node is empty");
		}

		// cluster was not absorbed, so write it out to the DB and
		// add it as a new leaf entry.
		Entry e = new Entry(cluster);
		leaf.addChild(e);
		size++;
		e.parent = leaf;
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

	private void printStats(Node n) {
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

		void absorb(ClusandraKernel target) {
			// before absorbing the given cluster, set the new density based on
			// the creation time of the given cluster
			density = getDensity(target.getCT());
			cluster.merge(target);
			center = Arrays.copyOf(cluster.getCenter(), center.length);
			N = cluster.getN();
		}

		// remove this Entry from the tree. Entries are found only in leaf
		// nodes!
		void remove() {
			parent.children.remove(this);
			Node nextParent = parent;
			while (nextParent != root) {
				nextParent.subCenter(this.N, this.center);
				nextParent = nextParent.parent;
			}
		}

		boolean isRelevant(double time) {
			return (getDensity(time) / getMaximumDensity()) >= getSparseFactor();
		}

		boolean isRelevant() {
			return isRelevant(System.currentTimeMillis());
		}

		/**
		 * Based on the given time (ms since epoc), return the temporal density
		 * of the given cluster.
		 * 
		 * @param entry
		 * @param time
		 *            If time is set to 0, then the current time is used.
		 * @return
		 */
		private double getDensity(double time) {
			if (time == 0.0d) {
				time = System.currentTimeMillis();
			} else if (time <= this.cluster.getLAT()) {
				return this.density;
			}
			// transform the times from milliseconds to seconds
			time = time / 1000;
			double lat = this.cluster.getLAT() / 1000;

			return ((Math.pow(getLambda(), Math.round(time - lat)) * this.density) + 1.0d);
		}

	}
}
