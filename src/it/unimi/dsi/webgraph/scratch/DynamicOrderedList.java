package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.logging.ProgressLogger;

/**
 * This class implements a simplified one-level version of the data structure described in Bender,
 * Michael A., et al. " Two simplified algorithms for maintaining order in a list." <i>European
 * Symposium on Algorithms</i>. Springer, Berlin, Heidelberg, 2002. All elements (of type
 * <code>K</code>) after insertions are dealt with through {@linkplain DOLNode suitable handles},
 * that are themselves nodes of a doubly-linked list, contain a refence to the actual element and
 * have an additional long tag that serves the purpose of comparison.
 *
 * <p>
 * The total order is initially empty, a new element can be
 * {@linkplain #insertAfter(DOLNode, Object) inserted} or {@linkplain #moveAfter(DOLNode, DOLNode)
 * moved} just after a given element, or it can be {@linkplain #delete(DOLNode) deleted}. At any
 * time one can test {@linkplain #compare(DOLNode, DOLNode) in which order} two given elements are.
 * Insertions/moves take O(log n) amortized time, whereas deletions and queries take O(1) time.
 *
 *
 * @param <K> the type of the elements contained in this list.
 */
public class DynamicOrderedList<K> {
	/** Number of bits used for tags (it cannot be larger than 63, because tags are longs).
	 *  The list cannot contain more than 1&lt;&lt;{@link #TAG_SIZE} elements.
	 */
	private final static int TAG_SIZE = 63;

	/** Transforms a tag into a 0-padded binary string of {@link #TAG_SIZE} bits.
	 *
	 * @param tag the tag to be transformed.
	 * @return the resulting binary string (of length {@link #TAG_SIZE}).
	 */
	private static String toBinary(final long tag) {
		final String s = Long.toBinaryString(tag);
		return ("0000000000000000" + s).substring(s.length());
	}

	/** A node of the doubly-linked list underlying this data structure. Nodes are compared by the numeric
	 *  comparison of their tags.
	 *
	 * @param <K> the type of element contained in this node.
	 */
	@SuppressWarnings("null")
	public static class DOLNode<K> implements Comparable<DOLNode<K>> {
		/** The actual content. */
		public K content;

		/** Reference to the previous node. */
		public DOLNode<K> prev;

		/** Reference to the next node. */
		public DOLNode<K> next;

		/** The tag of this node. */
		public long tag;

		@Override
		public String toString() {
			final String tag = toBinary(this.tag);
			if (this.prev == null) return "FIRST(" + tag + ")";
			if (this.next == null) return "LAST(" + tag + ")";
			return content.toString() + "(" + tag + ")";
		}

		@Override
		public int compareTo(final DOLNode<K> x) {
			return DynamicOrderedList.compare(this, x);
		}
	}

	/** Compares two nodes (through their tags).
	 *
	 * @param u the first node.
	 * @param v the second node.
	 * @return a negative, null, positive value depending on whether <code>u</code> comes before,
	 * coincides or comes after <code>v</code>.
	 */
	public static <K> int compare(final DOLNode<K> u, final DOLNode<K> v) {
		return Long.compare(u.tag, v.tag);
	}


	/** Fictitious element at the beginning of the list.  */
	public DOLNode<K> head;

	/** Fictitious element at the endof the list.  */
	public DOLNode<K> tail;

	/** No more than <code>1&lt;lt;k * Math.pow(overflowThreshold, TAG_SIZE-k)</code> elements can
	 *  share the same <code>k</code>-bit prefix.
	 */
	private final double overflowThreshold;

	/** Number of elements currently in the list. */
	public long size;

	/**
	 * Creates an empty list with given overflow threshold. No more than
	 * <code>1&lt;lt;k * Math.pow(overflowThreshold, TAG_SIZE-k)</code> elements can share the same
	 * <code>k</code>-bit prefix.
	 *
	 * @param overflowThreshold the overflow threshold of this list: it must be a number between 1 and
	 *            2.
	 */
	public DynamicOrderedList(final double overflowThreshold) {
		this.overflowThreshold = overflowThreshold;
		head = new DOLNode<>();
		tail = new DOLNode<>();
		head.next = tail;
		head.tag = 0;
		tail.prev = head;
		tail.tag = -1L >>> 64 - TAG_SIZE;
	}

	/** Creates an empty list with 3/2-overflow threshold. */
	public DynamicOrderedList() {
		this(3.0 / 2.0);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		DOLNode<K> curr = head;
		do {
			sb.append(curr.toString());
			if (curr != tail) sb.append(" -> ");
			curr = curr.next;
		} while (curr != null);
		return sb.toString();
	}

	/** Deletes a node. The deleted node is returned.
	 *
	 * @param node the node to be deleted. It must not be {@link #head} or {@link #tail}.
	 * @return the deleted node.
	 */
	public DOLNode<K> delete(final DOLNode<K> node) {
		node.prev.next = node.next;
		node.next.prev = node.prev;
		return node;
	}


	/** Moves a node immediately after a given one.
	 *
	 * @param node a node of the list. It must not be {@link #head} or {@link #tail}.
	 * @param previous the node immediately after which <code>node</code> should be moved. It cannot be {@link #tail}.
	 * @return the node after being moved.
	 */
	public DOLNode<K> moveAfter(final DOLNode<K> node, final DOLNode<K> previous) {
		final DOLNode<K> deleted = delete(node);
		return insertAfter(previous, node.content, deleted);
	}


	/** Inserts a new node immediately after a given one.
	 *
	 * @param previous the node immediately after which the new node should be inserted. It cannot be {@link #tail}.
	 * @param y the content of the node to be inserted.
	 * @return the inserted node.
	 */
	public DOLNode<K> insertAfter(final DOLNode<K> previous, final K y) {
		return insertAfter(previous, y, null);
	}

	/** Inserts a node immediately after a given one. Instead of creating a new node, an already
	 *  existing node is used.
	 *
	 * @param previous the node immediately after which the new node should be inserted. It cannot be {@link #tail}.
	 * @param y the content of the node to be inserted.
	 * @param newNode the node to be used.
	 * @return the inserted node.
	 */
	private DOLNode<K> insertAfter(final DOLNode<K> previous, final K y, DOLNode<K> newNode) {
		if (newNode == null) newNode = new DOLNode<>();
		newNode.content = y;
		newNode.next = previous.next;
		previous.next.prev = newNode;
		previous.next = newNode;
		newNode.prev = previous;
		size++;

		if (previous.tag + 1 != newNode.next.tag) {
			//newNode.tag = (previous.tag + newNode.next.tag) >>> 1;
			newNode.tag = previous.tag + 1;
			return newNode;
		}

		int lcpLength = Long.numberOfLeadingZeros(previous.tag ^ previous.next.tag) - (64 - TAG_SIZE);
		long lcpMask = -1L << (TAG_SIZE - lcpLength);
		long lcp = previous.tag & lcpMask;

		long occupied = 1;
		long count;
		DOLNode<K> left, right;
		left = previous;
		right = newNode.next;
		for (;;) {
			if (lcpLength == 0) {
				left = head;
				right = tail;
				count = 1L << TAG_SIZE;
				occupied = size;
				break;
			}

			while (left != head && (left.tag & lcpMask) == lcp) {
				left = left.prev;
				occupied++;
			}
			while (right != tail && (right.tag & lcpMask) == lcp) {
				right = right.next;
				occupied++;
			}
			count = (1L << (TAG_SIZE - lcpLength)) - 1;
			//System.err.println("Count: " + count + " lcp length: " + lcpLength);
			if (count >= Math.pow(overflowThreshold, TAG_SIZE - lcpLength) * occupied) break;
			lcpMask <<= 1;
			lcpLength--;
			lcp &= lcpMask;
		}

		final long step = count / (occupied + 1);
		//System.out.println(step);
		long t = lcp + step;
		//System.err.println(Long.toBinaryString(lcp));
		for (DOLNode<K> curr = left.next; curr != right; curr = curr.next) {
			curr.tag = t;
			t += step;
		}
		//System.out.println(this);
		return newNode;
	}

	/** Asserts that all tags are in order.
	 *
	 * @return true if all tags are in order.
	 */
	public boolean assertList() {
		for (DOLNode<K> curr = head; curr != tail; curr = curr.next) {
			assert curr.tag < curr.next.tag : toBinary(curr.tag) + ">=" + toBinary(curr.next.tag);
			assert curr == head || curr.prev.next == curr;
			assert curr == tail || curr.next.prev == curr;
		}

		for(int i = 0; i < TAG_SIZE - 1; i++)
			for (DOLNode<K> curr = head; curr != tail; curr = curr.next)
				assert curr.tag < curr.next.tag;
		return true;
	}

	public static void main(final String arg[]) {
		final DynamicOrderedList<String> list = new DynamicOrderedList<>(3.0 / 2.0);
		final ProgressLogger pl = new ProgressLogger();
		pl.start();
		DOLNode<String> curr = list.head;
		for (int i = 0; i < 1_000_000; i++) {
			final DOLNode<String> inserted = list.insertAfter(curr, "");
			curr = inserted.next;
			if (curr == list.tail) curr = list.head;
		}
		pl.done(1_000_000);
	}
}
