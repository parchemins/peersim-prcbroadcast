package descent.transport;

import java.util.ArrayList;

import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.PriorityQ;

public class FIFO implements PriorityQ {

	public ArrayList<ArrayList<Event>> time;
	public Integer size;

	public FIFO() {
		this.size = 0;
		this.time = new ArrayList<ArrayList<Event>>();
	}

	public FIFO(String prefix) {
		this.size = 0;
		this.time = new ArrayList<ArrayList<Event>>();
	}

	public void add(long time, Object event, Node node, byte pid) {
		Event toAdd = new Event();
		toAdd.event = event;
		toAdd.time = time;
		toAdd.pid = pid;
		toAdd.node = node;

		++this.size;
		int i = 0;
		while (i < this.time.size() && time > this.time.get(i).get(0).time) {
			++i;
		}

		if (i < this.time.size() && this.time.get(i).get(0).time == time) {
			this.time.get(i).add(toAdd);
		} else {
			this.time.add(i, new ArrayList<PriorityQ.Event>());
			this.time.get(i).add(toAdd);
		}
	}

	public void add(long time, Object event, Node node, byte pid, long priority) {
		this.add(time, event, node, pid);
	}

	public long maxPriority() {
		return Long.MAX_VALUE - 100;
	}

	public long maxTime() {
		return Long.MAX_VALUE;
	}

	public Event removeFirst() {
		if (this.size == 0)
			return null;

		--this.size;

		Event toReturn = this.time.get(0).get(0);
		this.time.get(0).remove(0);
		if (this.time.get(0).isEmpty())
			this.time.remove(0);
		return toReturn;
	}

	public int size() {
		return this.size();
	}

}
