/*
 * Created on Feb 2, 2006
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.algo.HyperBall;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

public class ComputeHopPlotVariance {

	private final static Logger LOGGER = LoggerFactory.getLogger(ComputeHopPlotVariance.class);

	public static double hopPlotVariance(ImmutableGraph graph, int log2m, final int iteration, final boolean random) throws IOException {
		HyperBall anf = new HyperBall(graph, log2m, new ProgressLogger(LOGGER));

		anf.init();

		double avgsquare, avg;
		double prev = 0;
		avgsquare = avg = 0;

		for (int i = 0; ; i++) {
			anf.iterate();
			double d = anf.neighbourhoodFunction.getDouble(anf.neighbourhoodFunction.size() - 1);
			if(random)
				System.out.println("R\t" + iteration + "\t" + i + "\t" + d);
			else
				System.out.println("G\t" + iteration + "\t" + i + "\t" + d);

			LOGGER.debug("The current iteration of ANF produced " + d);
			if ((i > 3 && d - prev <= 1E-3) || (i > 3 && d / prev < 1.001)) break;
			prev = d;
		}
		anf.close();
		int p = anf.neighbourhoodFunction.size();
		double mx = anf.neighbourhoodFunction.getDouble(p - 1);
		prev = 0;
		int t = 0;
		for (double v: anf.neighbourhoodFunction) {
			double frac = (v - prev) / mx;
			LOGGER.debug("P[" + t + "]=" + frac);
			prev = v;
			avgsquare += t * t * frac;
			avg += t * frac;
			t++;
		}
		return avgsquare - avg * avg;
	}

	@SuppressWarnings("boxing")
	public static void main(String[] args) throws JSAPException, IOException {
		SimpleJSAP jsap = new SimpleJSAP(ComputeHopPlotVariance.class.getName(), "Computes and outputs the hop-plot variance of a given graph.",
				new Parameter[] {
					new Switch("normalize", 'n', "normalize", "Normalize with respect to a random graph."),
					new FlaggedOption("log2m", JSAP.INTEGER_PARSER, "6", JSAP.NOT_REQUIRED, 'l', "log2m", "Logarithm (in base 2) of the numbers of registers per node."),
					new FlaggedOption("iterations", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'i', "iterations", "Number of iterations to compute the random-graph variance."),
					new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The graph basename"),
				}
			);

		JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("graph");
		ImmutableGraph graph = ImmutableGraph.load(basename);
		boolean normalize = jsapResult.getBoolean("normalize");
		int log2m = jsapResult.getInt("log2m");
		int iterations = jsapResult.getInt("iterations");


		double sum = 0;
		for(int i = 0; i < iterations; i++)
			sum += hopPlotVariance(graph, log2m, i, false);

		double variance = sum / iterations;

		if (normalize) {
			double sumRandom = 0;
			for (int i = 0; i < iterations; i++)
				sumRandom += hopPlotVariance(new ErdosRenyiGraph(graph.numNodes(), graph.numArcs(), 0, true), log2m, i, true);
			System.out.printf("%f\n", variance / (sumRandom / iterations));
		}

		else System.out.printf("%f\n", variance);
	}

}
