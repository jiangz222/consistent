consistent
==========

Consistent hash package for Go.

Installation
------------

    go get stathat.com/c/consistent

Examples
--------

Look at the [godoc](http://godoc.org/stathat.com/c/consistent).

Status
------

This package was extracted from production code powering [StatHat](http://www.stathat.com),
so clearly we feel that it is production-ready, but it should still be considered
experimental as other uses of it could reveal issues we aren't experiencing.

Contact us
----------

We'd love to hear from you if you are using `consistent`.
Get in touch:  [@stathat](http://twitter.com/stathat) or [contact us here](http://www.stathat.com/docs/contact).

About
-----

Written by Patrick Crosby at [StatHat](http://www.stathat.com).  Twitter:  [@stathat](http://twitter.com/stathat)

Changes in [jiangz222/consistent](https://github.com/jiangz222/consistent)
-----
- Support custom hash function in Config to New instance
- Support default number of replicas in Config to New instance
- Support number of replicas for every single member(both in Add and Remove)
```go
	x := New(newConfig())
	x.Add("def", 40)
    
```
- Add function: SetWithReplicas(),MemberReplicas()

 
