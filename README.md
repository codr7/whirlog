# ![Lisp Mascot](lisp.png?raw=true) Whirlog

### Introduction
Whirlog is a basic, single process, multi threaded, log-based relational database implemented in Common Lisp.
The design is based on a [Lisp tutorial]() I wrote, but adds several significant features to allow it to cover more than the simplest cases.

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
Contexts are completely independent, atomic transactions.
Feel free to start as many as you like, nested, or in parallel threads.

### Versions
All stored versions of a record are loaded into RAM.
`find-record` has a separate `:index` parameter that allows indexing in reverse order, `0` being the default /  most recent; a `NIL` result is returned once you hit the end.

### Records
Records are implemented as immutable lists of pairs, aka. association lists or alists; and written as is to disk. This means that any readable/writeable value is allowed as field value, and that the log is human readable as well as easy to process programatically.

```
("ben_dover")((WHIRLOG::PASSWORD . "badum") (WHIRLOG::USERNAME . "ben_dover"))
("ben_dover")((WHIRLOG::PASSWORD . "dish") (WHIRLOG::USERNAME . "ben_dover"))
("ben_dover")WHIRLOG::D
```
