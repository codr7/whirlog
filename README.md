# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a minimalistic single process, mt capable, versioned, log structured relational database implemented in Common Lisp.

```
  (let-tables ((tbl (key :primary-key? t) val))
    (with-db (nil (tbl))
	(let ((rec (new-record 'key "foo" 'val "bar")))
	  (assert (equal '("foo") (record-key rec tbl)))
	  
	  (do-context ()
	    (store-record tbl rec)
            (assert (string= (column-value (find-record tbl '("foo")) 'val) "bar")))
	  
	  (do-context ()
            (let ((rec (set-column-values rec 'val "baz")))
              (store-record tbl rec))

            (assert (string= (column-value (find-record tbl '("foo")) 'val) "baz"))))
	  
	  (do-context ()
	    (delete-record tbl '("foo"))
	    (assert (null (find-record tbl '("foo")))))))
```

### Databases
Databases are implemented as directories containing one file per table. `with-db` may be used to indicate a root path and open/close specified tables.

### Contexts
Contexts are independent atomic transactions. Changes are committed on success and rolled back on error by default, but the behavior may be customized by manually calling `commit-changes` and/or `rollback-changes` as needed.

### Tables
Tables are implemented as persistent, ordered trees of lazy loaded records.
Each table has a set of columns and a key.

#### Keys
Keys are compared using `whirlog:compare-column` which defaults to `rb:compare`.

### Records
Records are implemented as immutable lists of pairs (or alists); and written as is to disk. This means that any readable/writeable value will do as field value, and that log files are human readable as well as trivial to process.

```
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::VAL . "bar"))
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::VAL . "baz"))
("foo"):D
```

#### Versioning
Each logged version of a record (as identified by its key) is available on demand.

### Threads
Threaded table access has to be protected either by enclosing in `do-sync` or passing `:sync? t` (which is the default) where appropriate. Calls that require exclusive table access will eventually fail with an error unless they're able to acquire a table specific spinlock implemented using SBCL atomics.