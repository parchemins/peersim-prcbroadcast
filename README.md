# peersim-prcbroadcast

_Keywords: causal broadcast, reliable broadcast, space complexity, large and
dynamic systems_

[PeerSim](http://peersim.sourceforge.net/) [1] implementation for preventive
reliable causal broadcast. It exploits causal order to improve the underlying
reliable broadcast. Its space complexity becomes non-monotonic and depends on
the system and its current usage. In dynamic systems where processes join,
leave, or self reconfigure at any time, this comes at the cost of lightweight
control messages. Fortunately, the number of control messages stays small when
the peer-sampling protocol allows a form of routing.


## References

[1] A. Montresor and M. Jelasity. Peersim: A scalable P2P
simulator. _Proceedings of the 9th International Conference on Peer-to-Peer
(P2P’09)_, Seattle, WA, Sep. 2009, pp. 99-100.


