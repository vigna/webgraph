package it.unimi.dsi.webgraph.scratch;

import java.util.ArrayList;

import it.unimi.dsi.webgraph.scratch.DynamicOrderedList.DOLNode;

/** This class represents a dynamic DAG (nodes and arcs can be added but not deleted), keeping
 *  at the same time a topological order of its nodes, as described in:
 *  Haeupler, Bernhard, et al. "Incremental cycle detection, topological ordering, and strong component maintenance." <i>ACM Transactions on Algorithms</i> (TALG) 8.1 (2012): 3.
 *  Only Limited-Search (Fig. 1) is implemented.
 *
 * @param <K> the type of nodes.
 */
public class DynamicDAG<K> {

	/** The underlying topological order of nodes. */
	public DynamicOrderedList<DAGNode<K>> dynamicOrderedList;

	/** The type of a DAG node. */
	@SuppressWarnings("null")
	public static class DAGNode<K> {
		/** The node content. */
		public K content;

		/** The node successors (that must be already part of the topological order). */
		public ArrayList<DOLNode<DAGNode<K>>> successors;

		@Override
		public String toString() {
			return content.toString();
		}

	}

	/** Creates an empty DAG.
	 *
	 * @param overflowThreshold the threshold to be used to {@linkplain DynamicOrderedList#DynamicOrderedList(double) when building the dynamic ordered list}.
	 */
	public DynamicDAG(final double overflowThreshold) {
		dynamicOrderedList = new DynamicOrderedList<>(overflowThreshold);
	}

	/** Adds a new node.
	 *
	 * @param content the node content.
	 * @return the newly created node (as part of the topological order).
	 */
	public DOLNode<DAGNode<K>> addNode(final K content) {
		final DAGNode<K> dagNode = new DAGNode<>();
		dagNode.content = content;
		dagNode.successors = new ArrayList<>();
		return dynamicOrderedList.insertAfter(dynamicOrderedList.head, dagNode);
	}

	/** Adds an arc, if needed (does nothing if the arc is already present).
	 *
	 * @param source arc source.
	 * @param target arc target.
	 * @return <code>true</code> if the arc was added (or there was no need to add it). False if
	 * adding the arc would create cycles.
	 */
	public boolean addArc(final DOLNode<DAGNode<K>> source, final DOLNode<DAGNode<K>> target) {
		final DOLNode<DAGNode<K>> lastInserted = visitForwardAndMove(target, source, source);
		if (lastInserted == null) return false;
		addSuccessor(source, target);
		return true;
	}

	/** Visit all nodes starting from <code>node</code> in depth-first order, exluding those
	 *  that are larger (in the topological order) than <code>upperBound</code>. The nodes
	 *  are inserted, as soon as they are found, after <code>insertAfter</code>. If <code>upperBound</code> is
	 *  found during the visit, <code>null</code> is returned and the visit itself is interrupted.
	 *
	 * @param node the node from which the visit should start.
	 * @param upperBound only consider nodes that do not follow this one in the topological order.
	 * @param insertAfter node after which nodes should be inserted.
	 * @return the node that was moved last (or <code>null</code> if <code>upperBound</code> was reached).
	 */
	private DOLNode<DAGNode<K>> visitForwardAndMove(final DOLNode<DAGNode<K>> node, final DOLNode<DAGNode<K>> upperBound, DOLNode<DAGNode<K>> insertAfter) {
		final int comp = DynamicOrderedList.compare(node, upperBound);
		if (comp > 0) return insertAfter;
		if (comp == 0) return null; // cycle
		insertAfter = dynamicOrderedList.moveAfter(node, insertAfter);
		for (final DOLNode<DAGNode<K>> dolNode : node.content.successors) {
			insertAfter = visitForwardAndMove(dolNode, upperBound, insertAfter);
			if (insertAfter == null) return null;
		}
		return insertAfter;
	}

	/** Adds <code>w</code> among the successors of <code>v</code>, unless it is already present.
	 *
	 * @param v a node.
	 * @param w another node.
	 */
	private void addSuccessor(final DOLNode<DAGNode<K>> v, final DOLNode<DAGNode<K>> w) {
		if (!v.content.successors.contains(w)) v.content.successors.add(w);
	}

	@Override
	public String toString() {
		return dynamicOrderedList.toString();
	}

	public static void main(final String[] args) {
		final DynamicDAG<String> dag = new DynamicDAG<>(3.0 / 2.0);
		final DOLNode<DAGNode<String>> ciao = dag.addNode("ciao");
		final DOLNode<DAGNode<String>> mamma = dag.addNode("mamma");
		final DOLNode<DAGNode<String>> cane = dag.addNode("cane");
		boolean ok;
		System.out.println("***Now going to insert arc ciao->mamma");
		ok = dag.addArc(ciao, mamma);
		System.out.println(ok + " " + dag.dynamicOrderedList);
		System.out.println("***Now going to insert arc mamma->cane");
		ok = dag.addArc(mamma, cane);
		System.out.println(ok + " " + dag.dynamicOrderedList);
		System.out.println("***Now going to insert arc cane->ciao");
		ok  = dag.addArc(cane, ciao);
		System.out.println(ok + " " + dag.dynamicOrderedList);
	}

}
