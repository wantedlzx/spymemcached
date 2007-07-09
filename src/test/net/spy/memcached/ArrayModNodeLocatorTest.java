package net.spy.memcached;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test the ArrayModNodeLocator.
 */
public class ArrayModNodeLocatorTest extends AbstractNodeLocationCase {

	@Override
	protected void setupNodes(int n) {
		super.setupNodes(n);
		locator=new ArrayModNodeLocator(Arrays.asList(nodes),
			HashAlgorithm.NATIVE_HASH);
	}

	public void testPrimary() throws Exception {
		setupNodes(4);
		assertSame(nodes[1], locator.getPrimary("dustin"));
		assertSame(nodes[0], locator.getPrimary("x"));
		assertSame(nodes[1], locator.getPrimary("y"));
	}

	public void testAll() throws Exception {
		setupNodes(4);
		Collection<MemcachedNode> all = locator.getAll();
		assertEquals(4, all.size());
		assertTrue(all.contains(nodes[0]));
		assertTrue(all.contains(nodes[1]));
		assertTrue(all.contains(nodes[2]));
		assertTrue(all.contains(nodes[3]));
	}

	public void testSeq1() {
		setupNodes(4);
		assertSequence("dustin", 2, 3, 0);
	}

	public void testSeq2() {
		setupNodes(4);
		assertSequence("noelani", 1, 2, 3);
	}

	public void testSeqOnlyOneServer() {
		setupNodes(1);
		assertSequence("noelani");
	}

	public void testSeqWithTwoNodes() {
		setupNodes(2);
		assertSequence("dustin", 0);
	}
}
