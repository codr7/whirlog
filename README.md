# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a basic, single process, multi threaded, log-based relational database implemented in Common Lisp.
The design is based on a [Lisp tutorial](https://github.com/codr7/whirlisp) I wrote, but adds several significant features that allows it to cover more than the simplest cases.

```
  (let-tables ((users (username :primary-key? t) password))
    (with-db (nil users)
	(let ((rec (new-record 'username "ben_dover"
			       'password "badum")))
	  (assert (equal '("ben_dover") (record-key rec users)))
	  
	  (do-context ()
	    (store-record users rec)
            (assert (string= (column-value (find-record users '("ben_dover")) 'password)
                             "badum")))
	  
	  (do-context ()
            (let ((rec (set-column-values rec 'password "dish")))
              (store-record users rec))

            (assert (string= (column-value (find-record users '("ben_dover")) 'password)
                             "dish"))))
	  
	  (do-context ()
	    (delete-record users '("ben_dover"))
	    (assert (null (find-record users '("ben_dover")))))))
```

### Contexts
Contexts are completely independent, atomic transactions. Feel free to start as many as you like, nested, and/or in parallel threads.

### Tables
Any second now...
#### Keys
Any second now...

### Records
Records are implemented as immutable lists of pairs, aka. association lists or alists; and written as is to disk. This means that any readable/writeable value will do as field value, and that table logs are human readable as well as easy to process programatically.

```
("ben_dover")((WHIRLOG::PASSWORD . "badum") (WHIRLOG::USERNAME . "ben_dover"))
("ben_dover")((WHIRLOG::PASSWORD . "dish") (WHIRLOG::USERNAME . "ben_dover"))
("ben_dover"):D
```

#### Versions
All stored versions (since the last delete) of a record (identified by its primary key) are stacked and hashed on primary key per table in RAM. `find-record` takes a separate `:index`-parameter that allows indexing in reverse order, `0` being the default /  most recent version; `NIL` is returned once you hit the end.

### Threads
All threaded table access except for writes (store/delete) has to be protected either by enclosing in `do-sync` or by passing appropriate `:sync?`-arguments. `commit-changes` needs exclusive table access and will eventually fail with an error if it fails to acquire a table-specific spinlock implemented using SBCL atomics.