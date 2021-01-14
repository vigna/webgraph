package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

/** A graph with two parameters: a positive integer (<var>k</var>) and an integer value between 1 and <var>k<sup>2</sup></var>. The graph is
 *  bidirectional and made by two <var>k</var>-cliques with the specified number of edges in between them.
 *
 */
public class GeneralizedDumbelGraph extends ImmutableGraph {

	private final int k;
	private final int bridge;
	private final int[] result;

	public GeneralizedDumbelGraph(int k, int bridge) {
		this.k = k;
		this.bridge = bridge;
		if (k < 1 || bridge < 1 || bridge > k * k) throw new IllegalArgumentException("k must be a positive integer, and bridge must be between 1 and k^2");
		this.result = new int[2 * k];
	}

	public GeneralizedDumbelGraph(String... strings) {
		this(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]));
	}

	@Override
	public ImmutableGraph copy() {
		return null;
	}

	@Override
	public int numNodes() {
		return 2 * k;
	}

	@Override
	public int outdegree(int x) {
		if (x % 2 == 0) {
			if (x / 2 < bridge / k) return k + k;
			else if (x / 2 == bridge / k) return k + bridge % k;
			else return k;
		} else {
			if (x / 2 < bridge % k) return k + bridge / k + 1;
			else return k + bridge / k;
		}
	}

	@Override
	public int[] successorArray(int x) {
		int i;
		for (i = 0; i < k; i++)
			result[i] = 2 * i + x % 2; // Neighbors in the same clique
		if (x % 2 == 0) {
			if (x / 2 < bridge / k)
				for (int j = 0; j < k; j++)
					result[i++] = 2 * j + 1;
			else if (x / 2 == bridge / k)
					for (int j = 0; j < bridge % k; j++)
						result[i++] = 2 * j + 1;
		} else {
			for (int j = 0; j < bridge / k; j++)
				result[i++] = 2 * j;
			if (x / 2 < bridge % k)
				result[i++] = 2 * (bridge / k);
		}
		return result;
	}

	@Override
	public boolean randomAccess() {
		// TODO Auto-generated method stub
		return true;
	}

	public static void main(String arg[]) {
		int k = Integer.parseInt(arg[0]);
		int bridge = Integer.parseInt(arg[1]);
		ImmutableGraph g = new GeneralizedDumbelGraph(k, bridge);
		System.out.println("graph {");
		NodeIterator it = g.nodeIterator();
		while (it.hasNext()) {
			int from = it.nextInt();
			int d = it.outdegree();
			int[] succ = it.successorArray();
			for (int i = 0; i < d; i++)
				if (from < succ[i]) System.out.println(from + "--" + succ[i]);
		}
		System.out.println("}");
	}

}
