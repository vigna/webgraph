package it.unimi.dsi.webgraph.tool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.io.Files;
import com.martiansoftware.jsap.JSAPException;

import it.unimi.dsi.fastutil.io.BinIO;

public class ExtractComponentTest {

	@Test
	public void test() throws IOException, JSAPException {
		final File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		final File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();
		final File inIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-inIds");
		inIds.deleteOnExit();
		final File outIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-outIds");
		outIds.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		Files.asCharSink(inIds, StandardCharsets.US_ASCII).writeLines(Arrays.asList(new String[] { "a", "b", "c", "d",
				"e", "f", "g", "h" }));
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString(), inIds.toString(), outIds.toString() });

		assertArrayEquals(new int[] { -1, 0, -1, 1, 2, -1, -1, 3 }, BinIO.loadInts(mapFile));
		assertEquals(Arrays.asList(new String[] { "b", "d", "e",
				"h" }), Files.asCharSource(outIds, StandardCharsets.US_ASCII).readLines());

		componentsFile.delete();
		mapFile.delete();
		inIds.delete();
		outIds.delete();
	}

	@Test
	public void testNoIds() throws IOException, JSAPException {
		final File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		final File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString() });

		assertArrayEquals(new int[] { -1, 0, -1, 1, 2, -1, -1, 3 }, BinIO.loadInts(mapFile));

		componentsFile.delete();
		mapFile.delete();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testDifferentLengths() throws IOException, JSAPException {
		final File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		final File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();
		final File inIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-inIds");
		inIds.deleteOnExit();
		final File outIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-outIds");
		outIds.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		Files.asCharSink(inIds, StandardCharsets.US_ASCII).writeLines(Arrays.asList(new String[] { "a", "b", "c", "d",
				"e", "f", "g", "h", "i" }));
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString(), inIds.toString(), outIds.toString() });
	}
}
