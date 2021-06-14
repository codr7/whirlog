(defpackage whirlog
  (:use cl)
  (:import-from sb-ext compare-and-swap)
  (:import-from sb-thread thread-yield)
  (:import-from util dohash)
  (:export close-table column column-count column-value columns commit-changes context
	   delete-record do-context do-records do-sync
	   file find-record
	   let-tables
	   name new-column new-context new-table
	   open-table
	   primary-key primary-key?
	   read-records record-count records rollback-changes
	   set-column-value store-record
	   table
	   with-db
	   tests))

(in-package whirlog)

(defparameter *delete* :D)
(defparameter *eof* (gensym))

(defvar *context*)
(defvar *path* #P"")

(defun new-context ()
  "Returns a fresh context"
  (make-hash-table :test 'equal))

(defun push-change (tbl key rec)
  "Pushes change for REC with KEY in TBL"
  (setf (gethash (cons tbl key) *context*) rec))

(defun delete? (rec)
  "Returns T if REC is a delete"
  (eq rec *delete*))

(defun write-value (out x)
  "Writes X to OUT"
  (write x :stream out))

(defun rollback-changes ()
  "Rolls back changes in current context"
  (clrhash *context*))

(defmacro do-sync ((tbl &optional slots) &body body)
  `(with-slots (busy? ,@slots) ,tbl
     (tagbody
      lock
	(unless (eq (compare-and-swap busy? nil t) nil)
	  (thread-yield)
	  (go lock)))
     
     (unwind-protect
	  (progn ,@body)
       (tagbody
	unlock
	  (unless (eq (compare-and-swap busy? t nil) t)
	    (thread-yield)
	    (go unlock))))))

(defun table-records (tbl key &key (sync? t))
  "Returns stack of records for KEY in TBL"
  (with-slots (records) tbl
    (if sync?
	(do-sync (tbl) (rb:get-node key records))
	(rb:get-node key records))))

(defun (setf table-records) (val tbl key &key (sync? t))
  "Sets stack of records for KEY in TBL to VAL"
  (with-slots (records) tbl
    (if sync?
	(do-sync (tbl) (setf (rb:get-node key records) val))
	(setf (rb:get-node key records) val))))

(defun find-table-record (tbl key &key (sync? t))
  "Returns record for KEY in TBL, or NIL if not found"
  (first (table-records tbl key :sync? sync?)))

(defun commit-changes (&key (retries 3))
  "Commits changes in current context"
  (let (done)
    (labels ((undo ()
	       (dolist (c done)
		 (destructuring-bind (op tbl key rec) c
		   (do-sync (tbl (file records))
		     (ecase op
		       (:store
			(pop (table-records tbl key :sync? nil)))
		       (:delete 
			(push rec (table-records tbl key :sync? nil))))

		     (write-value file key)
		     (write-value file rec)
		     (terpri file)
		     (force-output file))))
	       (setf done nil)
	       nil)
	     (check ()
	       (dohash ((tbl . key) rec *context*)
		 (let ((trec (find-table-record tbl key)))
		   (unless (or (eq rec *delete*) (equal trec rec))
		     (if (zerop retries)
			 (error "Commit failed: ~a ~a" trec rec) 
			 (undo))
		     (return-from check nil))))
	       t))
      (handler-case
	  (progn 
	    (dohash ((tbl . key) rec *context*)
	      (do-sync (tbl (file records))
		(write-value file key)
		(write-value file rec)
		(terpri file)
		(force-output file)
		
		(if (delete? rec)
		    (push (list :delete tbl key (pop (table-records tbl key :sync? nil))) done)
		    (progn
		      (push rec (table-records tbl key :sync? nil))
		      (push (list :store tbl key *delete*) done)))))

	    (if (check)
		(rollback-changes)
		(progn
		  (thread-yield)
		  (commit-changes :retries (decf retries)))))
	(t (e)
	  (undo)
	  (error e))))))

(defmacro do-context (() &body body)
  `(let ((*context* (new-context)))
     (handler-case
	 (progn
	   ,@body
	   (commit-changes))
       (t (e)
	 (rollback-changes)
	 (error e)))))

(defclass table ()
  ((name :initarg :name
         :reader name)
   (busy? :initform nil
	  :reader busy?)
   (primary-key :initarg :primary-key
		:reader primary-key)	 
   (columns :initarg :columns
            :reader columns)
   (file :initarg :file
         :reader file)
   (records :initform (rb:new-root)
            :reader records)))

(defun new-table (name &rest cols)
  "Returns new table with NAME and COLS"
  (make-instance 'table
                 :name name
		 :primary-key (remove-if-not #'primary-key? cols)
                 :columns cols))

(defun read-value (in)
  "Reads value from IN"
  (read in nil *eof*))

(defun eof? (x)
  "Returns T if X is EOF"
  (eq x *eof*))

(defun read-records (tbl in &key (sync? t))
  "Reads records from IN into TBL"
  (with-slots (records) tbl
    (let ((key (read-value in)))
      (unless (eof? key)
        (let ((rec (read-value in)))
          (when (eof? rec)
  	    (error "Missing record for key ~a" key))
	  (if (delete? rec)
	      (setf (table-records tbl key :sync? sync?) nil)
              (push rec (table-records tbl key :sync? sync?)))
          (read-records tbl in))))))

(defun trim-path (in)
  (string-trim "*" in))

(defun open-table (tbl)
  "Opens and assigns TBL's file"
  (let ((path (merge-pathnames *path* (string-downcase (trim-path (symbol-name (name tbl)))))))
    (ensure-directories-exist path)
    (let ((file (open (format nil "~a.tbl" path)
		      :direction :io
		      :if-exists :overwrite
		      :if-does-not-exist :create)))
      (setf (slot-value tbl 'file) file)
      (read-records tbl file))))

(defun close-table (tbl)
  "Closes and unbinds TBL's file"
  (close (file tbl))
  (slot-makunbound tbl 'file))

(defun column-count (tbl)
  "Returns number of columns in TBL"
  (length (columns tbl)))

(defun record-count (tbl)
  "Returns number of records in TBL"
  (rb:node-count (records tbl)))

(defmacro do-records ((rec tbl &key start) &body body)
  (let (($key (gensym)) ($recs (gensym)))
    `(rb:do-tree (,$key ,$recs (if ,start (rb:find-node ,start (records ,tbl)) (rb:root-node (records ,tbl))))
       (declare (ignore ,$key))
       (let ((,rec (first ,$recs)))
	 ,@body))))

(defclass column ()
  ((name :initarg :name
         :reader name)
   (primary-key? :initarg :primary-key?
		 :initform nil
                 :reader primary-key?)))

(defun new-column (name &rest opts)
  "Returns new columns for NAME and OPTS"
  (apply #'make-instance
         'column
         :name name
	 opts))

(defun column-value (rec col)
  "Returns value for COL in REC"
  (rest (assoc col rec)))

(defun set-column-values (rec &rest flds)
  "Returns REC with updated FLDS"
  (labels ((acc (in out)
	     (if in
		 (acc (rest (rest in))
		      (cons (cons (first in) (second in))
			    out))
		 out)))
    (acc flds rec)))

(defun new-record (&rest flds)
  "Returns new record with FLDS"
  (apply #'set-column-values nil flds))

(defun record-key (rec tbl)
  "Returns key for REC in TBL"
  (mapcar (lambda (c)
            (column-value rec (name c)))
          (primary-key tbl)))

(defmacro with-db ((path &rest tbls) &body body)
  (let (($tbl (gensym))
	($tbls (gensym)))
    `(let ((,$tbls (list ,@tbls))
	   (*path* (merge-pathnames *path* ,(or path #P""))))
       (dolist (,$tbl ,$tbls)
	 (open-table ,$tbl))
       (unwind-protect
	    (progn ,@body)
	 (dolist (,$tbl ,$tbls)
	   (close-table ,$tbl))))))

(defmacro let-tables ((&rest tables) &body body)
  (labels ((bind (name &rest cols)
	     `(,name (new-table ',name
				,@(mapcar (lambda (c)
					    (if (listp c)
						`(new-column ',(first c) ,@(rest c))
						`(new-column ',c)))
					  cols)))))
    `(let (,@(mapcar (lambda (x) (apply #'bind x)) tables)) 
       ,@body)))

(defun store-record (tbl rec)
  "Stores REC in TBL"
  (let ((rec (remove-duplicates rec :key #'first :from-end t)))
    (push-change tbl (record-key rec tbl) rec)))

(defmacro if-changed ((tbl key var) x y)
  (let (($rec (gensym)))
    `(let ((,$rec (gethash (cons ,tbl ,key) *context*)))
       (if ,$rec
	   (let ((,var ,$rec)) ,x)
	   ,y))))

(defun find-record (tbl key &key (version 0) (sync? t))
  "Returns record for KEY in TBL if found, otherwise NIL"
  (let ((rec (if-changed (tbl key rec)
			 rec
			 (nth version (table-records tbl key :sync? sync?)))))
    (unless (delete? rec) rec)))

(defun delete-record (tbl key)
  "Deletes REC from TBL"
  (unless (find-record tbl key)
    (error "Record not found: ~a ~a" (name tbl) key))
  (push-change tbl key *delete*))

(defun delete-if-exists (path)
  (when (probe-file path)
    (delete-file path)))

(defun test-setup ()
  (delete-if-exists "/tmp/whirlog/users.tbl"))

(defmacro with-test-db ((&rest tables) &body body)
  `(with-db ("/tmp/whirlog/" ,@tables) ,@body))

(defun table-tests ()
  (test-setup)
  
  (let-tables ((users (username :primary-key? t) password))
    (assert (string= (name users) 'users))
    (assert (= (column-count users) 2))
    (assert (eq (name (first (primary-key users))) 'username))
    (with-test-db (users)
      (assert (= (record-count users) 0)))))

(defun record-tests ()
  (test-setup)
  
  (let-tables ((users (username :primary-key? t) password))
    (with-test-db (users)
      (let ((rec (new-record 'username "foo"
			     'password "bar")))
	(assert (equal '("foo") (record-key rec users)))
	
	(do-context ()
	  (store-record users rec)
          (assert (string= (column-value (find-record users '("foo")) 'password)
                           "bar")))
	
	(do-context ()
          (let ((rec (set-column-values rec 'password "baz")))
            (store-record users rec))

          (assert (string= (column-value (find-record users '("foo")) 'password)
                           "baz"))))
      
      (do-context ()
	(delete-record users '("foo"))
	(assert (null (find-record users '("foo"))))))))

(defun reload-tests ()
  (test-setup)
  
  (let-tables ((users (username :primary-key? t) password))
    (with-test-db (users)
      (let ((rec (new-record 'username "foo" 'password "bar")))
	(do-context ()
	  (store-record users rec))))

    (with-test-db (users)
      (do-context ()
	(assert (string= (column-value (find-record users '("foo")) 'password) "bar"))))))

(defun tests ()
  (table-tests)
  (record-tests)
  (reload-tests))

