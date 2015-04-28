package descent.cyclon;

import java.util.Comparator;

import peersim.core.Node;

/**
 * Created by julian on 3/14/15.
 */
public class CyclonEntry implements Comparable<CyclonEntry>,
		Comparator<CyclonEntry> {

	public int age;
	public Node n;

	protected CyclonEntry() {
	}

	public CyclonEntry(int age, Node n) {
		this.age = age;
		this.n = n;
	}

	public int compareTo(CyclonEntry ce) {
		if (ce.age > this.age) {
			return 1;
		} else if (ce.age == this.age) {
			return 0;
		} else {
			return -1;
		}
	}

	public int compare(CyclonEntry ce1, CyclonEntry ce2) {
		if (ce1.age > ce2.age) {
			return 1;
		} else if (ce1.age == ce2.age) {
			return 0;
		} else {
			return -1;
		}
	}

	public CyclonEntry cyclonCopy() {
		return new CyclonEntry(this.age, this.n);
	}

	@Override
	public String toString() {
		return "{" + n.getID() + "|age:" + this.age + "}";
	}
}