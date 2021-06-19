# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a minimalistic single process, multi threaded, versioned, log-based relational database implemented in Common Lisp.

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
Databases are implemented as directories containing one file per table. `with-db` may be used to indicate a path and open specified tables there.

### Contexts
Contexts are independent atomic transactions. Changes are committed on success and rolled back on error by default, but the behavior may be customized by manually calling `commit-changes` and/or `rollback-changes` as needed.

### Tables
A table is a persistent, ordered tree of records.

#### Keys
Each table has a set of columns, some of which form its primary key. Record keys are ordered using `whirlog:column-compare`which defaults to `rb:compare`-ing the values.

### Records
Records are implemented as immutable lists of pairs, aka. association lists or alists; and written as is to disk. This means that any readable/writeable value will do as field value, and that table logs are human readable as well as easy to process programatically.

```
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::VAL . "bar"))
("foo")((WHIRLOG::KEY . "foo") (WHIRLOG::VAL . "baz"))
("foo"):D
```

#### Versioning
Each logged version of a record (as identified by its key) is available on demand.

### Threads
Threaded table access has to be protected either by enclosing in `do-sync` or leaving `:sync?`-argument defaulted. Calls that require exclusive table access will eventually fail with an error unless they're able to acquire a table specific spinlock implemented using SBCL atomics.