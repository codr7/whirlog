# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a basic, single process, multi threaded, log-based relational database implemented in Common Lisp.
The design is based on a [Lisp tutorial](https://github.com/codr7/whirlisp) I wrote, but adds several significant features that allows it to cover more than the simplest cases.

```
  (let-tables ((tbl (key :primary-key? t) val))
    (with-db (nil tbl)
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
Databases are implemented as directories containing one file per table. `with-db` may be used to indicate a path and open specified tables from there.

### Contexts
Contexts are completely independent, atomic transactions. Feel free to start as many as you like, nested, and/or in parallel threads. Contexts are committed on success and rolled back on errors by default, but the behavior may be customized by manually calling `commit-changes` and/or `rollback-changes` as needed.

### Tables
A table is a persistent, ordered collection of records.

#### Keys
Each table has a set of columns, some of which form its primary key. Record keys are ordered using `whirlog:column-compare`which defaults to `rb:compare`-ing the values.

### Records
Records are implemented as immutable lists of pairs, aka. association lists or alists; and written as is to disk. This means that any readable/writeable value will do as field value, and that table logs are human readable as well as easy to process programatically.

```
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::VAL . "bar"))
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::KEY . "baz"))
("foo"):D
```

#### Versions
All stored versions of a record (as identified by its primary key) are stacked and indexed per table in RAM. `find-record` takes a `:version`-parameter that allows indexing the stack, `0` being default and most recent.

### Threads
All threaded table access except for writes (store/delete) has to be protected either by enclosing in `do-sync` or by passing appropriate `:sync?`-arguments. `commit-changes` needs exclusive table access and will eventually fail with an error if it can't acquire a table-specific spinlock implemented using SBCL atomics.