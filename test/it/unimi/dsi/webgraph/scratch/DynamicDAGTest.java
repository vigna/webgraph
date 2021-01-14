package it.unimi.dsi.webgraph.scratch;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponents;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;
import it.unimi.dsi.webgraph.scratch.DynamicDAG.DAGNode;
import it.unimi.dsi.webgraph.scratch.DynamicOrderedList.DOLNode;



//RELEASE-STATUS: DIST

public class DynamicDAGTest {

	@SuppressWarnings("boxing")
	@Test
	public void testPath() {
		final DynamicDAG<Integer> dag = new DynamicDAG<>(3.0 / 2.0);
		final int n = 25;
		boolean ok;

		final int[] nodeOrder = IntArrays.shuffle(Util.identity(n), new XoRoShiRo128PlusRandom(0));
		@SuppressWarnings("unchecked")
		final
		DOLNode<DAGNode<Integer>>[] x = new DOLNode[n];
		for (int i = 0; i < n; i++) {
			x[nodeOrder[i]] = dag.addNode(nodeOrder[i]);
			Assert.assertTrue(dag.dynamicOrderedList.assertList());
		}
		final int[] arcOrder = IntArrays.shuffle(Util.identity(n), new XoRoShiRo128PlusRandom(0));
		for (int i = 0; i < n; i++)
			if (arcOrder[i] < n - 1) {
				ok = dag.addArc(x[arcOrder[i]], x[arcOrder[i] + 1]);
				Assert.assertTrue(ok);
				Assert.assertTrue(dag.dynamicOrderedList.assertList());
			}
		// Now test for order
		for (int i = 0; i < n; i++)
			for (int j = 0; j < n; j++)
				Assert.assertEquals(Integer.compare(i, j), DynamicOrderedList.compare(x[i], x[j]));

	}

	@Ignore
	@SuppressWarnings("boxing")
	@Test
	public void testERTree() throws IOException {
		final int n = 1000;
		final String tmpBasename = File.createTempFile(DynamicDAGTest.class.getSimpleName(), "test").toString();
		BVGraph.store(new ErdosRenyiGraph(n, 0.003), tmpBasename);
		final BVGraph erGraph = BVGraph.load(tmpBasename);
		final StronglyConnectedComponents scc = StronglyConnectedComponents.compute(erGraph, false, null);
		final ImmutableGraph dag = Transform.map(erGraph, scc.component);
		final int nDag = dag.numNodes();

		final DynamicDAG<Integer> ddag = new DynamicDAG<>(3.0 / 2.0);
		@SuppressWarnings("unchecked")
		final
		DOLNode<DAGNode<Integer>>[] x = new DOLNode[nDag];
		for (int i = 0; i < nDag; i++) x[i] = ddag.addNode(i);
		NodeIterator nodeIterator = dag.nodeIterator();
		while (nodeIterator.hasNext()) {
			final int source = nodeIterator.nextInt();
			int target;
			final LazyIntIterator successors = nodeIterator.successors();
			while ((target = successors.nextInt()) >= 0)
				ddag.addArc(x[source], x[target]);
		}
		nodeIterator = dag.nodeIterator();
		while (nodeIterator.hasNext()) {
			final int source = nodeIterator.nextInt();
			int target;
			final LazyIntIterator successors = nodeIterator.successors();
			while ((target = successors.nextInt()) >= 0)
				Assert.assertTrue(DynamicOrderedList.compare(x[source], x[target]) < 0 );
		}
		System.out.println(nDag + " " + scc.numberOfComponents);
	}

}
