# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a minimal versioned log structured relational DB implemented in Common Lisp.

```
  (let-tables ((tbl (key :key? t) val))
    (with-db ("/tmp/db/" (tbl))
	(let ((rec (new-record 'key "foo" 'val "bar")))
	  (do-context ()
	    (store-record tbl rec)
            (assert (string= (column-value (find-record tbl #("foo")) 'val) "bar")))
	  
	  (do-context ()
            (let ((rec (set-column-values rec 'val "baz")))
              (store-record tbl rec))

            (assert (string= (column-value (find-record tbl #("foo")) 'val) "baz"))))
	  
	  (do-context ()
	    (delete-record tbl #("foo"))
	    (assert (null (find-record tbl #("foo")))))))
```

You may find a more real worldish example [here](https://github.com/codr7/shedulr/blob/main/shedulr.lisp).

### Databases
Databases are implemented as directories containing one file per table. `with-db` may be used to indicate a root path and open/close specified tables.

### Contexts
Contexts are independent atomic transactions. Changes are committed on success and rolled back on error by default, but the behavior may be customized by manually calling `commit-changes` and/or `rollback-changes` as needed.

### Tables
Tables are implemented as persistent, ordered trees of lazy loaded records.
Each table has a set of columns and a key.

#### Columns
Columns may be typed or untyped (default), untyped columns compare their values using `sort:compare`.

```
  (let-tables ((tbl (id :type string :key? t)
                    (parent :type record :table tbl)))
    ...)
```

##### lset-column
`lset` columns automatically encode sets as lists and decode back to sets again, sets are compared by value.

##### number-column
Number columns compare values as numbers.

##### string-column
String columns compare values as strings.

##### record-column
Record columns automatically encode records as keys (vectors) which are compared by value.

#### Records
Records are implemented as immutable lists of pairs (or alists); and written as is to disk. This means that any readable/writeable value will do as field value, and that log files are human readable as well as trivial to process.

```
#("foo")((WHIRLOG::VAL . "bar"))
#("foo")((WHIRLOG::VAL . "baz"))
#("foo"):D
```

##### Keys
Record keys are implemented as vectors and compared by value.

#### Versioning
Each logged version of a record (as identified by its key) is available on demand.

### Threads
Threaded table access has to be protected either by enclosing in `do-sync` or passing `:sync? t` (which is the default) where appropriate. Calls that require exclusive table access will eventually fail with an error unless they're able to acquire a table specific spinlock implemented using SBCL atomics.