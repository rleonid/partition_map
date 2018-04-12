Partition Maps
------------------------

  TL;DR: A partition map is a data structure to track associations where we
  privilege merging above other operations.


Introduction
------------

A partition map is a way to represent a function
(_f_ : _D_ -> _R_), an association, a map.

There are many data structures that one can use to represent functions such as
arrays, association-lists, trees, hash-tables and variants of these. Often, the
deciding factor of which implementation to use depends upon the stored data and 
desired access pattern. For example, most data structures privilege value
setting and getting. A partition map, is a different technique, where we want
to prioritize _merging_ above other access patterns.

When we merge, we want to take two functions (_f<sub>1</sub>_, _f<sub>2</sub>_),
and compute a new one, ex. _f<sub>1</sub> + f<sub>2</sub>_.
But we must maintain the property that for every element in the domain we store
the correct value in the range.

Under some useful conditions:

  1. The size of the range (_R_) is much smaller than the domain (_D_).
     For example, let _D_ = [1,100] and _f = 1_ if x in [1,10] or [90,100]
     and _f = 2_ if x in 
      
  2. The merging operation is _bounded_; it does not grow the range in
 

<a name="technique_footnote">1:</a>This is still very much a work in progress,
and I'm not certain that the current method and implementation is the best one.
A big motivation for publishing and exposing this work is to gather feedback.
